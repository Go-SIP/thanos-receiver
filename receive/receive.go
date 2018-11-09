package receive

import (
	"io"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"

	"github.com/improbable-eng/thanos/pkg/store/prompb"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

// Appendable returns an Appender.
type Appendable interface {
	Appender() (storage.Appender, error)
}

type Receiver struct {
	logger log.Logger
	append Appendable
	cache  *refCache
}

func NewReceiver(logger log.Logger, app Appendable) *Receiver {
	return &Receiver{
		logger: logger,
		append: app,
		cache:  newRefCache(),
	}
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

func (r *Receiver) Receive(wreq *PartialWriteRequest) error {
	app, err := r.append.Appender()
	if err != nil {
		return err
	}

	for _, t := range wreq.Timeseries {
		//l := len(t.LabelBytes[0] )
		//for i := 1; i < len(t.LabelBytes); i++ {
		//l += len(t.LabelBytes[i])
		//}
		//metric := t.LabelBytes[0]
		//for i := 1; i < len(t.LabelBytes); i++ {
		//	metric = append(metric, t.LabelBytes[i]...)
		//}
		//cacheEntry, ok := r.cache.get(yoloString(metric))
		//if ok {
		//	for _, s := range t.Samples {
		//		err = app.AddFast(cacheEntry.lset, cacheEntry.ref, s.Timestamp, s.Value)
		//		if err != nil {
		//			return err
		//		}
		//	}
		//}
		//if !ok {
		err = t.UnmarshalLabels()
		if err != nil {
			return errors.Wrap(err, "failed to unmarshal labels")
		}
		lset := make(labels.Labels, len(t.Labels))
		for j := range t.Labels {
			lset[j] = labels.Label{
				Name:  t.Labels[j].Name,
				Value: t.Labels[j].Value,
			}
		}
		//hash := lset.Hash()

		//var ref uint64
		for _, s := range t.Samples {
			//ref, err = app.Add(lset, s.Timestamp, s.Value)
			_, err = app.Add(lset, s.Timestamp, s.Value)
			if err != nil {
				return err
			}
		}

		//r.cache.addRef(string(metric), ref, lset, hash)
		//}
	}

	if err := app.Commit(); err != nil {
		return err
	}

	return nil
}

const (
	timeSeriesStart = 0xa
	valueStart      = 0x12
	labelStart      = 0xa
)

type TimeSeries struct {
	LabelSetBytes []byte
	Samples       []*prompb.Sample
}

func parseWriteRequest(writeRequestBytes []byte) ([]*TimeSeries, error) {
	t := []*TimeSeries{}

	for i := 0; i < len(writeRequestBytes); {
		if writeRequestBytes[i] != timeSeriesStart {
			return nil, io.ErrUnexpectedEOF
		}
		i++
		if i >= len(writeRequestBytes) {
			return nil, io.ErrUnexpectedEOF
		}
		size := uint64(writeRequestBytes[i]) // should this size really just be a uint64?
		maxIndex := i + int(size)
		if maxIndex >= len(writeRequestBytes) {
			return nil, io.ErrUnexpectedEOF
		}
		i++
		if i >= len(writeRequestBytes) {
			return nil, io.ErrUnexpectedEOF
		}

		labelsStartIndex := i
		for writeRequestBytes[i] != valueStart {
			if i >= maxIndex {
				return nil, io.ErrUnexpectedEOF
			}
			if writeRequestBytes[i] != labelStart {
				return nil, io.ErrUnexpectedEOF
			}
			i++
			if i >= len(writeRequestBytes) {
				return nil, io.ErrUnexpectedEOF
			}
			size := uint64(writeRequestBytes[i])
			i = i + int(size) + 1
			if i >= len(writeRequestBytes) {
				return nil, io.ErrUnexpectedEOF
			}
		}
		labelsEndIndex := i

		samples := []*prompb.Sample{}
		for i < len(writeRequestBytes) && writeRequestBytes[i] != timeSeriesStart {
			if i > maxIndex {
				return nil, io.ErrUnexpectedEOF
			}
			if i == maxIndex {
				break
			}
			if writeRequestBytes[i] != valueStart {
				return nil, io.ErrUnexpectedEOF
			}
			i++
			if i >= len(writeRequestBytes) {
				return nil, io.ErrUnexpectedEOF
			}
			size := uint64(writeRequestBytes[i])
			i++
			if i >= len(writeRequestBytes) {
				return nil, io.ErrUnexpectedEOF
			}
			sampleStartIndex := i
			sampleEndIndex := i + int(size)
			if sampleEndIndex > len(writeRequestBytes) {
				return nil, io.ErrUnexpectedEOF
			}
			samples = append(samples, &prompb.Sample{})
			err := samples[len(samples)-1].Unmarshal(writeRequestBytes[sampleStartIndex:sampleEndIndex])
			if err != nil {
				return nil, err
			}
			i = i + int(size)
			if i == len(writeRequestBytes) {
				break
			}
			if i >= len(writeRequestBytes) {
				return nil, io.ErrUnexpectedEOF
			}
		}

		t = append(t, &TimeSeries{
			LabelSetBytes: writeRequestBytes[labelsStartIndex:labelsEndIndex],
			Samples:       samples,
		})
	}

	return t, nil
}

func parseLabels(labelBytes []byte) (labels.Labels, error) {
	l := labels.Labels{}
	for i := 0; i < len(labelBytes); {
		if labelBytes[i] != labelStart {
			return nil, io.ErrUnexpectedEOF
		}
		i++
		if i >= len(labelBytes) {
			return nil, io.ErrUnexpectedEOF
		}
		size := uint64(labelBytes[i])
		i++
		if i >= len(labelBytes) {
			return nil, io.ErrUnexpectedEOF
		}
		labelStartIndex := i
		labelEndIndex := i + int(size)
		if labelEndIndex > len(labelBytes) {
			return nil, io.ErrUnexpectedEOF
		}
		label := &prompb.Label{}
		err := label.Unmarshal(labelBytes[labelStartIndex:labelEndIndex])
		if err != nil {
			return nil, err
		}
		l = append(l, labels.Label{
			Name:  label.Name,
			Value: label.Value,
		})
		i = i + int(size)
		if i == len(labelBytes) {
			break
		}
		if i > len(labelBytes) {
			return nil, io.ErrUnexpectedEOF
		}
	}

	return l, nil
}

type cacheEntry struct {
	ref  uint64
	hash uint64
	lset labels.Labels
}

type refCache struct {
	series map[string]*cacheEntry
}

func newRefCache() *refCache {
	return &refCache{
		series: map[string]*cacheEntry{},
	}
}

func (c *refCache) get(met string) (*cacheEntry, bool) {
	e, ok := c.series[met]
	if !ok {
		return nil, false
	}
	return e, true
}

func (c *refCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {
	if ref == 0 {
		return
	}
	c.series[met] = &cacheEntry{ref: ref, lset: lset, hash: hash}
}
