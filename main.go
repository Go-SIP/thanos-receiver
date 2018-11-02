package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/brancz/thanos-remote-receive/receive"
	"github.com/brancz/thanos-remote-receive/web"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/tsdb"
)

func main() {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = level.NewFilter(logger, level.AllowDebug())
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)
	registry := prometheus.NewRegistry()

	tsdbCfg := &tsdb.Options{
		Retention:        model.Duration(time.Hour * 24 * 15),
		NoLockfile:       true,
		MinBlockDuration: model.Duration(time.Hour * 2),
		MaxBlockDuration: model.Duration(time.Hour * 2),
	}

	ctxWeb, cancelWeb := context.WithCancel(context.Background())
	localStorage := &tsdb.ReadyStorage{}
	receiver := receive.NewReceiver(log.With(logger, "component", "receiver"), localStorage)
	webHandler := web.New(log.With(logger, "component", "web"), &web.Options{
		Context:        ctxWeb,
		Receiver:       receiver,
		ListenAddress:  ":19090",
		MaxConnections: 1000,
		ReadTimeout:    time.Minute * 5,
		Registry:       registry,
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

	var g run.Group
	{
		// Termination handler.
		term := make(chan os.Signal)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				// Don't forget to release the reloadReady channel so that waiting blocks can exit normally.
				select {
				case <-term:
					level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
					reloadReady.Close()

				case <-webHandler.Quit():
					level.Warn(logger).Log("msg", "Received termination request via web service, exiting gracefully...")
				case <-cancel:
					reloadReady.Close()
					break
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
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
				close(cancel)
			},
		)
	}
	{
		// TSDB.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "Starting TSDB ...")
				db, err := tsdb.Open(
					"data/",
					log.With(logger, "component", "tsdb"),
					registry,
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
				if err := localStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "Error stopping storage", "err", err)
				}
				close(cancel)
			},
		)
	}
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
				cancelWeb()
			},
		)
	}
	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "See you next time!")

}
