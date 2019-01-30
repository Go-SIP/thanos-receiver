package runnable

import (
	"context"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	"github.com/Go-SIP/thanos-receiver/receive"
	"github.com/Go-SIP/thanos-receiver/web"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	"github.com/oklog/run"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/tsdb"
	"github.com/prometheus/tsdb/labels"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func RegisterReceiver(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "receiver to accept remote write API requests.")

	grpcBindAddr, httpMetricsBindAddr, cert, key, clientCA, newPeerFn := regCommonServerFlags(cmd)

	remoteWriteAddress := cmd.Flag("remote-write.address", "Address to listen on for remote write requests.").
		Default("0.0.0.0:19291").String()

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		peer, err := newPeerFn(logger, reg, false, "", false)
		if err != nil {
			return errors.Wrap(err, "new cluster peer")
		}
		return RunReceiver(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*cert,
			*key,
			*clientCA,
			*httpMetricsBindAddr,
			*remoteWriteAddress,
			*dataDir,
			peer,
			name,
		)
	}
}

func RunReceiver(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	cert string,
	key string,
	clientCA string,
	httpMetricsBindAddr string,
	remoteWriteAddress string,
	dataDir string,
	peer cluster.Peer,
	component string,
) error {
	level.Info(logger).Log("msg", "setting up receiver")

	var metadata = &metadata{
		// Start out with the full time range. The shipper will constrain it later.
		// TODO(fabxc): minimum timestamp is never adjusted if shipping is disabled.
		mint: 0,
		maxt: math.MaxInt64,
	}

	tsdbCfg := &tsdb.Options{
		Retention:        model.Duration(time.Minute * 6),
		NoLockfile:       true,
		MinBlockDuration: model.Duration(time.Minute * 3),
		MaxBlockDuration: model.Duration(time.Minute * 3),
	}

	ctxWeb, cancelWeb := context.WithCancel(context.Background())
	localStorage := &tsdb.ReadyStorage{}
	receiver := receive.NewReceiver(log.With(logger, "component", "receiver"), localStorage)
	webHandler := web.New(log.With(logger, "component", "web"), &web.Options{
		Context:        ctxWeb,
		Receiver:       receiver,
		ListenAddress:  remoteWriteAddress,
		MaxConnections: 1000,
		ReadTimeout:    time.Minute * 5,
		Registry:       reg,
		ReadyStorage:   localStorage,
	})

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.
	dbOpen := make(chan struct{})

	// sync.Once is used to make sure we can close the channel at different execution stages(SIGTERM or when the config is loaded).
	type closeOnce struct {
		C     chan struct{}
		once  sync.Once
		Close func()
	}
	// Wait until the server is ready to handle reloading.
	reloadReady := &closeOnce{
		C: make(chan struct{}),
	}
	reloadReady.Close = func() {
		reloadReady.once.Do(func() {
			close(reloadReady.C)
		})
	}

	level.Debug(logger).Log("msg", "setting up joining cluster")
	{
		ctx, cancel := context.WithCancel(context.Background())
		g.Add(func() error {
			mint, maxt := metadata.Timestamps()
			if peer != nil {
				if err := peer.Join(cluster.PeerTypeSource, cluster.PeerMetadata{
					Labels:  metadata.LabelsPB(),
					MinTime: mint,
					MaxTime: maxt,
				}); err != nil {
					return errors.Wrap(err, "join cluster")
				}
			}

			<-ctx.Done()
			return nil
		}, func(err error) {
			level.Debug(logger).Log("msg", "mesh group errored", "err", err)
			cancel()
			peer.Close(2 * time.Second)
		})
	}

	level.Debug(logger).Log("msg", "setting up endpoint readiness")
	{
		// Initial configuration loading.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-dbOpen:
					break
				// In case a shutdown is initiated before the dbOpen is released
				case <-cancel:
					reloadReady.Close()
					return nil
				}

				reloadReady.Close()

				webHandler.Ready()
				level.Info(logger).Log("msg", "Server is ready to receive web requests.")
				<-cancel
				return nil
			},
			func(err error) {
				level.Debug(logger).Log("msg", "initial load group errored", "err", err)
				close(cancel)
			},
		)
	}

	level.Debug(logger).Log("msg", "setting up tsdb")
	{
		// TSDB.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "Starting TSDB ...")
				db, err := tsdb.Open(
					dataDir,
					log.With(logger, "component", "tsdb"),
					reg,
					tsdbCfg,
				)
				if err != nil {
					return fmt.Errorf("opening storage failed: %s", err)
				}
				level.Info(logger).Log("msg", "TSDB started")

				startTimeMargin := int64(2 * time.Duration(tsdbCfg.MinBlockDuration).Seconds() * 1000)
				localStorage.Set(db, startTimeMargin)
				close(dbOpen)
				<-cancel
				return nil
			},
			func(err error) {
				level.Debug(logger).Log("msg", "tsdb group errored", "err", err)
				if err := localStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "Error stopping storage", "err", err)
				}
				close(cancel)
			},
		)
	}

	level.Debug(logger).Log("msg", "setting up metric http listen-group")
	if err := metricHTTPListenGroup(g, logger, reg, httpMetricsBindAddr); err != nil {
		level.Debug(logger).Log("msg", "metric listener errored", "err", err)
		return err
	}

	level.Debug(logger).Log("msg", "setting up grpc server")
	{
		var (
			logger = log.With(logger, "component", "receiver")

			s   *grpc.Server
			l   net.Listener
			err error
		)
		g.Add(func() error {
			select {
			case <-dbOpen:
				break
			}

			l, err = net.Listen("tcp", grpcBindAddr)
			if err != nil {
				return errors.Wrap(err, "listen API address")
			}

			db := localStorage.Get()
			tsdbStore := store.NewTSDBStore(log.With(logger, "component", "thanos-tsdb-store"), reg, db, nil)

			opts, err := defaultGRPCServerOpts(logger, reg, tracer, cert, key, clientCA)
			if err != nil {
				return errors.Wrap(err, "setup gRPC server")
			}
			s = grpc.NewServer(opts...)
			storepb.RegisterStoreServer(s, tsdbStore)

			level.Info(logger).Log("msg", "Listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			level.Debug(logger).Log("msg", "grpc group errored", "err", err)
			if s != nil {
				s.Stop()
			}
			if l != nil {
				runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
			}
		})
	}

	level.Debug(logger).Log("msg", "setting up receive http handler")
	{
		// Web handler.
		g.Add(
			func() error {
				if err := webHandler.Run(ctxWeb); err != nil {
					return fmt.Errorf("error starting web server: %s", err)
				}
				return nil
			},
			func(err error) {
				level.Debug(logger).Log("msg", "receive http handler errored", "err", err)
				cancelWeb()
			},
		)
	}
	level.Info(logger).Log("msg", "starting receiver", "peer", peer.Name())

	return nil
}

type metadata struct {
	mtx    sync.Mutex
	mint   int64
	maxt   int64
	labels labels.Labels
}

func (s *metadata) UpdateTimestamps(mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	s.mint = mint
	s.maxt = maxt
}

func (s *metadata) Labels() labels.Labels {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.labels
}

func (s *metadata) LabelsPB() []storepb.Label {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	lset := make([]storepb.Label, 0, len(s.labels))
	for _, l := range s.labels {
		lset = append(lset, storepb.Label{
			Name:  l.Name,
			Value: l.Value,
		})
	}
	return lset
}

func (s *metadata) Timestamps() (mint int64, maxt int64) {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	return s.mint, s.maxt
}
