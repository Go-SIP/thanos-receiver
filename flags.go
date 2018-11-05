package main

import (
	"fmt"
	"io/ioutil"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"gopkg.in/alecthomas/kingpin.v2"
)

func regCommonServerFlags(cmd *kingpin.CmdClause) (
	grpcBindAddr *string,
	httpBindAddr *string,
	grpcTLSSrvCert *string,
	grpcTLSSrvKey *string,
	grpcTLSSrvClientCA *string,
	peerFunc func(log.Logger, *prometheus.Registry, bool, string, bool) (*cluster.Peer, error)) {

	grpcBindAddr = cmd.Flag("grpc-address", "Listen ip:port address for gRPC endpoints (StoreAPI). Make sure this address is routable from other components if you use gossip, 'grpc-advertise-address' is empty and you require cross-node connection.").
		Default("0.0.0.0:10901").String()

	grpcAdvertiseAddr := cmd.Flag("grpc-advertise-address", "Explicit (external) host:port address to advertise for gRPC StoreAPI in gossip cluster. If empty, 'grpc-address' will be used.").
		String()

	grpcTLSSrvCert = cmd.Flag("grpc-server-tls-cert", "TLS Certificate for gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvKey = cmd.Flag("grpc-server-tls-key", "TLS Key for the gRPC server, leave blank to disable TLS").Default("").String()
	grpcTLSSrvClientCA = cmd.Flag("grpc-server-tls-client-ca", "TLS CA to verify clients against. If no client CA is specified, there is no client verification on server side. (tls.NoClientCert)").Default("").String()

	httpBindAddr = regHTTPAddrFlag(cmd)

	clusterBindAddr := cmd.Flag("cluster.address", "Listen ip:port address for gossip cluster.").
		Default("0.0.0.0:10900").String()

	clusterAdvertiseAddr := cmd.Flag("cluster.advertise-address", "Explicit (external) ip:port address to advertise for gossip in gossip cluster. Used internally for membership only.").
		String()

	peers := cmd.Flag("cluster.peers", "Initial peers to join the cluster. It can be either <ip:port>, or <domain:port>. A lookup resolution is done only at the startup.").Strings()

	gossipInterval := modelDuration(cmd.Flag("cluster.gossip-interval", "Interval between sending gossip messages. By lowering this value (more frequent) gossip messages are propagated across the cluster more quickly at the expense of increased bandwidth. Default is used from a specified network-type.").
		PlaceHolder("<gossip interval>"))

	pushPullInterval := modelDuration(cmd.Flag("cluster.pushpull-interval", "Interval for gossip state syncs. Setting this interval lower (more frequent) will increase convergence speeds across larger clusters at the expense of increased bandwidth usage. Default is used from a specified network-type.").
		PlaceHolder("<push-pull interval>"))

	refreshInterval := modelDuration(cmd.Flag("cluster.refresh-interval", "Interval for membership to refresh cluster.peers state, 0 disables refresh.").Default(cluster.DefaultRefreshInterval.String()))

	secretKey := cmd.Flag("cluster.secret-key", "Initial secret key to encrypt cluster gossip. Can be one of AES-128, AES-192, or AES-256 in hexadecimal format.").HexBytes()

	networkType := cmd.Flag("cluster.network-type",
		fmt.Sprintf("Network type with predefined peers configurations. Sets of configurations accounting the latency differences between network types: %s.",
			strings.Join(cluster.NetworkPeerTypes, ", "),
		),
	).
		Default(cluster.LanNetworkPeerType).
		Enum(cluster.NetworkPeerTypes...)

	return grpcBindAddr,
		httpBindAddr,
		grpcTLSSrvCert,
		grpcTLSSrvKey,
		grpcTLSSrvClientCA,
		func(logger log.Logger, reg *prometheus.Registry, waitIfEmpty bool, httpAdvertiseAddr string, queryAPIEnabled bool) (*cluster.Peer, error) {
			host, port, err := cluster.CalculateAdvertiseAddress(*grpcBindAddr, *grpcAdvertiseAddr)
			if err != nil {
				return nil, errors.Wrapf(err, "calculate advertise StoreAPI addr for gossip based on bindAddr: %s and advAddr: %s", *grpcBindAddr, *grpcAdvertiseAddr)
			}

			advStoreAPIAddress := net.JoinHostPort(host, strconv.Itoa(port))
			if cluster.IsUnroutable(advStoreAPIAddress) {
				level.Warn(logger).Log("msg", "this component advertises its gRPC StoreAPI on an unroutable address. This will not work cross-cluster", "addr", advStoreAPIAddress)
				level.Warn(logger).Log("msg", "provide --grpc-address as routable ip:port or --grpc-advertise-address as a routable host:port")
			}

			level.Info(logger).Log("msg", "StoreAPI address that will be propagated through gossip", "address", advStoreAPIAddress)

			advQueryAPIAddress := httpAdvertiseAddr
			if queryAPIEnabled {
				host, port, err := cluster.CalculateAdvertiseAddress(*httpBindAddr, advQueryAPIAddress)
				if err != nil {
					return nil, errors.Wrapf(err, "calculate advertise QueryAPI addr for gossip based on bindAddr: %s and advAddr: %s", *httpBindAddr, advQueryAPIAddress)
				}

				advQueryAPIAddress = net.JoinHostPort(host, strconv.Itoa(port))
				if cluster.IsUnroutable(advQueryAPIAddress) {
					level.Warn(logger).Log("msg", "this component advertises its HTTP QueryAPI on an unroutable address. This will not work cross-cluster", "addr", advQueryAPIAddress)
					level.Warn(logger).Log("msg", "provide --http-address as routable ip:port or --http-advertise-address as a routable host:port")
				}

				level.Info(logger).Log("msg", "QueryAPI address that will be propagated through gossip", "address", advQueryAPIAddress)
			}

			return cluster.New(logger,
				reg,
				*clusterBindAddr,
				*clusterAdvertiseAddr,
				advStoreAPIAddress,
				advQueryAPIAddress,
				*peers,
				waitIfEmpty,
				time.Duration(*gossipInterval),
				time.Duration(*pushPullInterval),
				time.Duration(*refreshInterval),
				*secretKey,
				*networkType,
			)
		}
}

func regHTTPAddrFlag(cmd *kingpin.CmdClause) *string {
	return cmd.Flag("http-address", "Listen host:port for HTTP endpoints.").Default("0.0.0.0:10902").String()
}

func modelDuration(flags *kingpin.FlagClause) *model.Duration {
	var value = new(model.Duration)
	flags.SetValue(value)

	return value
}

type pathOrContent struct {
	name string

	path    *string
	content *string
}

func (p *pathOrContent) Content() ([]byte, error) {
	if len(*p.path) > 0 && len(*p.content) > 0 {
		return nil, errors.Errorf("Both file and content are set for %s", p.name)
	}

	if len(*p.path) > 0 {
		c, err := ioutil.ReadFile(*p.path)
		if err != nil {
			return nil, errors.Wrap(err, fmt.Sprintf("loading YAML file %s for %s", *p.path, p.name))
		}
		return c, nil
	}

	if len(*p.content) > 0 {
		return []byte(*p.content), nil
	}

	return nil, nil
}

func regCommonObjStoreFlags(cmd *kingpin.CmdClause, suffix string) *pathOrContent {
	fileFlagName := fmt.Sprintf("objstore%s.config-file", suffix)
	bucketConfFile := cmd.Flag(fileFlagName, fmt.Sprintf("Path to YAML file that contains object store%s configuration.", suffix)).
		PlaceHolder("<bucket.config-yaml-path>").String()

	bucketConf := cmd.Flag(fmt.Sprintf("objstore%s.config", suffix), fmt.Sprintf("Alternative to '%s' flag. Object store%s configuration in YAML.", fileFlagName, suffix)).
		PlaceHolder("<bucket.config-yaml>").String()

	return &pathOrContent{
		name: fmt.Sprintf("objstore%s.config", suffix),

		path:    bucketConfFile,
		content: bucketConf,
	}
}
