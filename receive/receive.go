package receive

import (
	"sync"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"

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

func unsafeString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

func (r *Receiver) Receive(wreq *PartialWriteRequest) error {
	app, err := r.append.Appender()
	if err != nil {
		return errors.Wrap(err, "failed to get appender")
	}

	for _, t := range wreq.Timeseries {
		var totalLen int
		for _, s := range t.LabelBytes {
			totalLen += len(s)
		}
		metric := make([]byte, totalLen)
		pos := 0
		for _, s := range t.LabelBytes {
			pos += copy(metric[pos:], s)
		}
		cacheEntry, ok := r.cache.get(unsafeString(metric))
		if ok {
			for _, s := range t.Samples {
				err = app.AddFast(cacheEntry.lset, cacheEntry.ref, s.Timestamp, s.Value)
				if err != nil {
					return errors.Wrap(err, "failed to fast add")
				}
			}
		}
		if !ok {
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
			hash := lset.Hash()

			var ref uint64
			for _, s := range t.Samples {
				ref, err = app.Add(lset, s.Timestamp, s.Value)
				_, err = app.Add(lset, s.Timestamp, s.Value)
				if err != nil {
					return errors.Wrap(err, "failed to non-fast add")
				}
			}

			r.cache.addRef(string(metric), ref, lset, hash)
		}
	}

	if err := app.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}

	return nil
}

type cacheEntry struct {
	ref  uint64
	hash uint64
	lset labels.Labels
}

type refCache struct {
	series map[string]*cacheEntry
	mtx    *sync.RWMutex
}

func newRefCache() *refCache {
	return &refCache{
		series: map[string]*cacheEntry{},
		mtx:    &sync.RWMutex{},
	}
}

func (c *refCache) get(met string) (*cacheEntry, bool) {
	c.mtx.RLock()
	defer c.mtx.RUnlock()

	e, ok := c.series[met]
	if !ok {
		return nil, false
	}
	return e, true
}

func (c *refCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if ref == 0 {
		return
	}
	c.series[met] = &cacheEntry{ref: ref, lset: lset, hash: hash}
}
