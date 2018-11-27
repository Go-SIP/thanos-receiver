// Copyright 2017 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/Go-SIP/thanos-receiver/runnable"
	"github.com/go-kit/kit/log"
	"github.com/golang/snappy"
	"github.com/improbable-eng/thanos/pkg/cluster"
	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/oklog/run"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/tsdb"
	"github.com/prometheus/tsdb/labels"
	"golang.org/x/net/context/ctxhttp"
	"gopkg.in/alecthomas/kingpin.v2"
)

func main() {
	var (
		cli                  = kingpin.New(filepath.Base(os.Args[0]), "CLI tool for tsdb")
		benchCmd             = cli.Command("bench", "run benchmarks")
		benchWriteCmd        = benchCmd.Command("write", "run a write performance benchmark")
		benchWriteOutPath    = benchWriteCmd.Flag("out", "set the output path").Default("benchout").String()
		benchWriteNumMetrics = benchWriteCmd.Flag("metrics", "number of metrics to read").Default("10000").Int()
		benchSamplesFile     = benchWriteCmd.Arg("file", "input file with samples data, default is ("+filepath.Join("testdata", "20kseries.json")+")").Default(filepath.Join("testdata", "20kseries.json")).String()
	)

	switch kingpin.MustParse(cli.Parse(os.Args[1:])) {
	case benchWriteCmd.FullCommand():
		wb := &writeBenchmark{
			outPath:     *benchWriteOutPath,
			numMetrics:  *benchWriteNumMetrics,
			samplesFile: *benchSamplesFile,
		}
		wb.run()
	}
	flag.CommandLine.Set("log.level", "debug")
}

type writeBenchmark struct {
	outPath     string
	samplesFile string
	cleanup     bool
	numMetrics  int

	storage *tsdb.DB

	cpuprof   *os.File
	memprof   *os.File
	blockprof *os.File
	mtxprof   *os.File
}

func (b *writeBenchmark) run() {
	if b.outPath == "" {
		dir, err := ioutil.TempDir("", "tsdb_bench")
		if err != nil {
			exitWithError(err)
		}
		b.outPath = dir
		b.cleanup = true
	}
	if err := os.RemoveAll(b.outPath); err != nil {
		exitWithError(err)
	}
	if err := os.MkdirAll(b.outPath, 0777); err != nil {
		exitWithError(err)
	}

	l := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	l = log.With(l, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller)

	var metrics [][]prompb.Label
	var err error
	var total uint64
	r := prometheus.NewRegistry()

	peerAddr := fmt.Sprintf("127.0.0.1:0")
	peer, err := cluster.New(
		log.NewNopLogger(),
		r,
		peerAddr,
		peerAddr,
		"sidecar-address",
		"",
		nil,
		false,
		100*time.Millisecond,
		50*time.Millisecond,
		30*time.Millisecond,
		nil,
		cluster.LanNetworkPeerType,
	)
	if err != nil {
		exitWithError(err)
	}

	measureTime("readData", func() {
		f, err := os.Open(b.samplesFile)
		if err != nil {
			exitWithError(err)
		}
		defer f.Close()

		metrics, err = readPrometheusLabels(f, b.numMetrics)
		if err != nil {
			exitWithError(err)
		}
	})

	var g run.Group
	runnable.RunReceiver(
		&g,
		l,
		r,
		nil,
		"0.0.0.0:10901",
		"",
		"",
		"",
		"0.0.0.0:10902",
		"data/",
		peer,
		"receiver",
	)
	cancel := make(chan struct{})
	g.Add(func() error {
		<-cancel
		return nil
	}, func(error) {
	})
	go func() {
		if err := g.Run(); err != nil {
			exitWithError(fmt.Errorf("bench: failed to run group: %v", err))
		}
	}()
	time.Sleep(5 * time.Second)
	dur := measureTime("ingestScrapes", func() {
		b.startProfiling()
		total, err = b.ingestScrapes(metrics, 3000)
		if err != nil {
			exitWithError(err)
		}
	})

	fmt.Println(" > total samples:", total)
	fmt.Println(" > samples/sec:", float64(total)/dur.Seconds())

	measureTime("stopStorage", func() {
		close(cancel)
		if err := b.stopProfiling(); err != nil {
			exitWithError(err)
		}
	})
}

const timeDelta = 30000

func (b *writeBenchmark) ingestScrapes(lbls [][]prompb.Label, scrapeCount int) (uint64, error) {
	var mu sync.Mutex
	var total uint64

	for i := 0; i < scrapeCount; i += 100 {
		var wg sync.WaitGroup
		lbls := lbls
		for len(lbls) > 0 {
			l := 1000
			if len(lbls) < 1000 {
				l = len(lbls)
			}
			batch := lbls[:l]
			lbls = lbls[l:]

			wg.Add(1)
			go func() {
				n, err := b.ingestScrapesShard(batch, 100, int64(timeDelta*i))
				if err != nil {
					fmt.Println(" err", err)
				}
				mu.Lock()
				total += n
				mu.Unlock()
				wg.Done()
			}()
		}
		wg.Wait()
	}
	fmt.Println("rendering write requests completed")

	return total, nil
}

func (b *writeBenchmark) ingestScrapesShard(metrics [][]prompb.Label, scrapeCount int, baset int64) (uint64, error) {
	ts := baset

	type sample struct {
		labels []prompb.Label
		value  float64
	}

	scrape := make([]*sample, 0, len(metrics))

	for _, m := range metrics {
		scrape = append(scrape, &sample{
			labels: m,
			value:  123456789,
		})
	}
	total := uint64(0)

	for i := 0; i < scrapeCount; i++ {
		ts += timeDelta

		writeRequest := &prompb.WriteRequest{
			Timeseries: make([]*prompb.TimeSeries, len(scrape)),
		}
		for j, s := range scrape {
			s.value += 1000

			writeRequest.Timeseries[j] = &prompb.TimeSeries{
				Labels: s.labels,
				Samples: []prompb.Sample{
					{
						Value:     s.value,
						Timestamp: ts,
					},
				},
			}

			total++
		}
		data, err := writeRequest.Marshal()
		if err != nil {
			exitWithError(fmt.Errorf("bench: could not marshall write request: %v", err))
		}

		compressed := snappy.Encode(nil, data)
		httpReq, err := http.NewRequest("POST", "http://localhost:19090/receive", bytes.NewReader(compressed))
		if err != nil {
			exitWithError(fmt.Errorf("bench: new http request error: %v", err))
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()

		httpReq.Header.Add("Content-Encoding", "snappy")
		httpReq.Header.Set("Content-Type", "application/x-protobuf")
		httpReq.Header.Set("X-Prometheus-Remote-Write-Version", "0.1.0")
		httpReq = httpReq.WithContext(ctx)

		httpResp, err := ctxhttp.Do(ctx, nil, httpReq)
		if err != nil {
			exitWithError(fmt.Errorf("bench: http request error: %v", err))
		}
		defer httpResp.Body.Close()

		if httpResp.StatusCode/100 != 2 {
			scanner := bufio.NewScanner(io.LimitReader(httpResp.Body, 1000))
			line := ""
			if scanner.Scan() {
				line = scanner.Text()
			}
			err = fmt.Errorf("server returned HTTP status %s: %s", httpResp.Status, line)
			exitWithError(fmt.Errorf("bench: non 2xx HTTP status: %v", err))
		}
		if httpResp.StatusCode/100 == 5 {
			exitWithError(fmt.Errorf("bench: 5xx HTTP status: %v", err))
		}
	}
	return total, nil
}

func (b *writeBenchmark) startProfiling() {
	var err error

	// Start CPU profiling.
	b.cpuprof, err = os.Create(filepath.Join(b.outPath, "cpu.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create cpu profile: %v", err))
	}
	if err := pprof.StartCPUProfile(b.cpuprof); err != nil {
		exitWithError(fmt.Errorf("bench: could not start CPU profile: %v", err))
	}

	// Start memory profiling.
	b.memprof, err = os.Create(filepath.Join(b.outPath, "mem.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create memory profile: %v", err))
	}
	runtime.MemProfileRate = 64 * 1024

	// Start fatal profiling.
	b.blockprof, err = os.Create(filepath.Join(b.outPath, "block.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create block profile: %v", err))
	}
	runtime.SetBlockProfileRate(20)

	b.mtxprof, err = os.Create(filepath.Join(b.outPath, "mutex.prof"))
	if err != nil {
		exitWithError(fmt.Errorf("bench: could not create mutex profile: %v", err))
	}
	runtime.SetMutexProfileFraction(20)
}

func (b *writeBenchmark) stopProfiling() error {
	if b.cpuprof != nil {
		pprof.StopCPUProfile()
		b.cpuprof.Close()
		b.cpuprof = nil
	}
	if b.memprof != nil {
		if err := pprof.Lookup("heap").WriteTo(b.memprof, 0); err != nil {
			return fmt.Errorf("error writing mem profile: %v", err)
		}
		b.memprof.Close()
		b.memprof = nil
	}
	if b.blockprof != nil {
		if err := pprof.Lookup("block").WriteTo(b.blockprof, 0); err != nil {
			return fmt.Errorf("error writing block profile: %v", err)
		}
		b.blockprof.Close()
		b.blockprof = nil
		runtime.SetBlockProfileRate(0)
	}
	if b.mtxprof != nil {
		if err := pprof.Lookup("mutex").WriteTo(b.mtxprof, 0); err != nil {
			return fmt.Errorf("error writing mutex profile: %v", err)
		}
		b.mtxprof.Close()
		b.mtxprof = nil
		runtime.SetMutexProfileFraction(0)
	}
	return nil
}

func measureTime(stage string, f func()) time.Duration {
	fmt.Printf(">> start stage=%s\n", stage)
	start := time.Now()
	f()
	fmt.Printf(">> completed stage=%s duration=%s\n", stage, time.Since(start))
	return time.Since(start)
}

func readPrometheusLabels(r io.Reader, n int) ([][]prompb.Label, error) {
	scanner := bufio.NewScanner(r)

	var mets [][]prompb.Label
	hashes := map[uint64]struct{}{}
	i := 0

	for scanner.Scan() && i < n {
		l := make(labels.Labels, 0, 10)

		r := strings.NewReplacer("\"", "", "{", "", "}", "")
		s := r.Replace(scanner.Text())

		labelChunks := strings.Split(s, ",")
		for _, labelChunk := range labelChunks {
			split := strings.Split(labelChunk, ":")
			l = append(l, labels.Label{Name: split[0], Value: split[1]})
		}
		// Order of the k/v labels matters, don't assume we'll always receive them already sorted.
		sort.Sort(l)
		h := l.Hash()
		if _, ok := hashes[h]; ok {
			continue
		}

		m := make([]prompb.Label, len(l))
		for j := range l {
			m[j] = prompb.Label{Name: l[j].Name, Value: l[j].Value}
		}

		mets = append(mets, m)
		hashes[h] = struct{}{}
		i++
	}
	return mets, nil
}

func exitWithError(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
