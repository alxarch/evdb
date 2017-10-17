package meter

import (
	"sync"
	"time"
)

// Sample holds the value at a specific time with second granularity
type Sample struct {
	Value     float64
	Timestamp int64
}

// Sampler is the interface for sampling values over time
type Sampler interface {
	Len() int
	Cap() int
	Diff(since time.Time, q int) float64
	Avg(since time.Time, q int) float64
	// Rate(depth time.Duration, q int) float64
	Observe(v float64)
}

type sampler struct {
	samples []Sample
	size    int
	start   int
	cap     int
}

var _ Sampler = &sampler{}

// NewSampler creates a non thread safe Sampler
func NewSampler(size int) Sampler {
	return newSampler(size)
}

func newSampler(size int) *sampler {
	return &sampler{
		samples: make([]Sample, size),
		cap:     size,
	}
}

// Cap returns the capacity of a sample circular buffer
func (s *sampler) Cap() int {
	return s.cap
}

// Len returns the number of samples in the circular buffer
func (s *sampler) Len() int {
	return s.size
}

// index finds the circular buffer index in a sample sequence
func (s *sampler) index(i int) int {
	if i > s.size {
		return -1
	}
	return (s.start + i) % s.cap
}

// Diff sums the diff between samples in a sample sequence
func (s *sampler) Diff(since time.Time, q int) (diff float64) {
	_, diff = s.diff(since.Unix(), q)
	return
}

// Avg finds the average diff of samples in a sample sequence
func (s *sampler) Avg(since time.Time, q int) float64 {
	if n, diff := s.diff(since.Unix(), q); n != 0 {
		return diff / float64(n)
	}
	return 0
}

// diff sums all sample diffs using avg diff at positions where monotony of samples changes
func (s *sampler) diff(since int64, q int) (n int, diff float64) {
	var i, j, k int
	var ti, tj int64
	var d, vi, vj float64
	for pos := s.size - 1; pos >= 0; pos-- {
		if j = s.index(pos - 1); j == -1 {
			return
		}
		i = s.index(pos)
		vi, vj = s.samples[i].Value, s.samples[j].Value
		d = vi - vj
		switch {
		case d > 0:
			switch k {
			case -1:
				vi = s.guessAt(pos, q)
				d = vi - vj
			case 0:
				k = 1
			}
		case d < 0:
			switch k {
			case 1:
				vi = s.guessAt(pos, q)
				d = vi - vj
			case 0:
				k = -1
			}
		default:
			continue
		}
		if tj = s.samples[j].Timestamp; tj < since {
			if ti = s.samples[i].Timestamp; ti == tj {
				return
			}
			d *= float64(ti-since) / float64(ti-tj)
			diff += d
			n++
			return
		}
		diff += d
		n++

	}
	return
}

// guessAt extrapolates a value for a sample based on previous monotonic samples' average diff
func (s *sampler) guessAt(pos int, n int) (v float64) {
	pos--
	if n, v = s.diffAt(pos, n); n != 0 {
		v = s.samples[s.index(pos)].Value + v/float64(n)
	} else if pos = s.index(pos); pos != -1 {
		v = s.samples[pos].Value
	}
	return
}

// diffAt finds the sum of diffs at a specific pos in the samples as long as
// the monotony of the values doesn't change
func (s *sampler) diffAt(pos int, maxdepth int) (n int, diff float64) {
	var i, j, k int
	var vi, vj, d float64
	if maxdepth < 0 {
		maxdepth = s.size - pos
	}
	for _ = pos; pos >= 0 && n < maxdepth; pos-- {
		if j = s.index(pos - 1); j == -1 {
			return
		}
		i = s.index(pos)
		vi, vj = s.samples[i].Value, s.samples[j].Value
		d = vi - vj
		switch {
		case d > 0:
			switch k {
			case -1:
				// Value monotony chanes at pos
				return
			case 0:
				// Record values' monotony
				k = 1
			}
		case d < 0:
			switch k {
			case 1:
				// Value monotony chanes at pos
				return
			case 0:
				// Record values' monotony
				k = -1
			}
		default:
			n++
			continue
		}
		diff += d
		n++
	}
	return
}

// ObserveAt records a sample at a specific point in time
func (s *sampler) ObserveAt(ts int64, v float64) {
	sample := Sample{v, ts}
	if s.size == s.cap {
		s.samples[s.start] = sample
		s.start++
		s.start = s.index(s.start)
	} else {
		s.samples[s.index(s.size)] = sample
		s.size++
	}
}

// ObserveAt records a sample at the current time
func (s *sampler) Observe(v float64) {
	s.ObserveAt(time.Now().Unix(), v)
}

// SafeSampler is a thread safe Sampler
type SafeSampler struct {
	*sampler
	mu sync.RWMutex
}

var _ Sampler = &SafeSampler{}

// NewSafeSampler initializes a SafeSampler
func NewSafeSampler(size int) *SafeSampler {
	return &SafeSampler{sampler: newSampler(size)}
}

// Observe records a value at the current time
func (s *SafeSampler) Observe(v float64) {
	s.mu.Lock()
	s.sampler.Observe(v)
	s.mu.Unlock()
}

// Len returns the number of samples in the circular buffer
func (s *SafeSampler) Len() (n int) {
	s.mu.RLock()
	n = s.sampler.Len()
	s.mu.RUnlock()
	return
}

// Diff returns the sum of sample diffs up to a specific point in time
func (s *SafeSampler) Diff(since time.Time, q int) (v float64) {
	s.mu.RLock()
	v = s.sampler.Diff(since, q)
	s.mu.RUnlock()
	return
}

// Avg returns the average of sample diffs up to a specific point in time
func (s *SafeSampler) Avg(since time.Time, q int) (avg float64) {
	s.mu.RLock()
	avg = s.sampler.Avg(since, q)
	s.mu.RUnlock()
	return
}

type EventSampler struct {
	*SafeSampler
	values []string
	desc   *Desc
}

func (s *EventSampler) Describe() *Desc {
	return s.desc
}
func (s *EventSampler) Values() []string {
	return s.values
}
func (s *EventSampler) LabelValues() (lvs LabelValues) {
	return s.desc.LabelValues(s.values)
}

type Samplers struct {
	samplers  map[string]*SafeSampler
	desc      *Desc
	size      int
	fieldSize int
	mu        sync.RWMutex
}

func NewSamplers(size int, desc *Desc) *Samplers {
	if desc == nil {
		return nil
	}
	if size < 1 {
		size = 1
	}
	labels := desc.Labels()
	s := Samplers{
		samplers:  make(map[string]*SafeSampler),
		desc:      desc,
		size:      size,
		fieldSize: 2 * len(labels),
	}
	for _, label := range labels {
		s.fieldSize += len(label)
	}
	return &s

}
func (s *Samplers) WithLabels(lvs LabelValues) *SafeSampler {
	values := lvs.Values(s.desc.Labels())
	return s.WithLabelValues(values...)
}

func (s *Samplers) WithLabelValues(values ...string) (ss *SafeSampler) {
	size := len(values) - 1
	for _, v := range values {
		size += len(v)
	}
	data := make([]byte, 0, size)
	for i, v := range values {
		if i != 0 {
			data = append(data, LabelSeparator)
		}
		data = append(data, v...)
	}
	s.mu.RLock()
	ss = s.samplers[string(data)]
	s.mu.RUnlock()
	if ss != nil {
		return
	}
	s.mu.Lock()
	if ss = s.samplers[string(data)]; ss != nil {
		s.mu.Unlock()
		return
	}
	ss = NewSafeSampler(s.size)
	s.samplers[string(data)] = ss
	s.mu.Unlock()
	return
}
