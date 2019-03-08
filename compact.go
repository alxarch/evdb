package meter

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

type RetentionPolicy map[time.Duration]time.Duration

func (p RetentionPolicy) Cascade(now time.Time) []TimeRange {
	rules := make([]RetentionRule, 0, len(p))
	for step, period := range p {
		rules = append(rules, RetentionRule{step, period})
	}
	return RetentionRules(rules).Normalize().Steps(now)
}

type RetentionRule struct {
	Step   time.Duration
	Period time.Duration
}
type RetentionRules []RetentionRule

func (rules RetentionRules) Normalize() RetentionRules {
	sort.SliceStable(rules, func(i, j int) bool {
		a, b := &rules[i], &rules[j]
		switch {
		case a.Step < b.Step:
			return true
		case a.Step > b.Step:
			return false
		default:
			return a.Period > b.Period
		}
	})
	out := rules[:0]
	last := RetentionRule{
		Step: time.Second,
	}
	for _, r := range rules {
		r.Step = r.Step.Truncate(last.Step)
		r.Period = r.Period.Truncate(r.Step)
		if r.Step <= last.Step || r.Period <= last.Period {
			continue
		}
		if r.Step%last.Step == 0 {
			out = append(out, r)
		} else {
			out[len(out)-1].Period = r.Period
		}
		last = r
	}
	return out
}

func (rules RetentionRules) get(i int) *RetentionRule {
	if 0 <= i && i < len(rules) {
		return &rules[i]
	}
	return nil
}

func (rules RetentionRules) Steps(now time.Time) []TimeRange {
	result := make([]TimeRange, len(rules))
	for i := range result {
		rule := rules.get(i)
		tr := &result[i]
		tr.Start = now.Add(-rule.Period)
		tr.Step = rule.Step
		if i == 0 {
			tr.End = now
		} else {
			tr.End = result[i-1].Start
		}
	}
	return result
}

type Compaction struct {
	Rules      RetentionPolicy
	NumWorkers int
}

const (
	prefixByteValue = 0
	prefixByteEvent = 1
)

func (b *badgerStore) Compaction(ctx context.Context, tm time.Time, numWorkers int, rules RetentionPolicy) error {
	steps := rules.Cascade(tm)
	if len(steps) == 0 {
		return nil
	}
	if numWorkers < 1 {
		numWorkers = 1
	}
	if ctx == nil {
		ctx = context.Background()
	}
	ch := make(chan error, numWorkers)
	ctx, cancel := context.WithCancel(ctx)
	wg := sync.WaitGroup{}
	wg.Add(numWorkers)
	type task struct {
		Start, End int64
		NumKeys    int
	}
	worker := func(tasks <-chan *task) {
		defer wg.Done()
		for t := range tasks {
			err := b.squash(t.Start, t.End, t.NumKeys)
			if err != nil {
				defer cancel() // cancel on first worker error
				ch <- err      // Non blocking, ch is buffered
				return
			}
		}
	}
	tasks := make(chan *task)
	for i := 0; i < numWorkers; i++ {
		go worker(tasks)
	}
	go b.View(func(txn *badger.Txn) (err error) {
		defer close(tasks) // closes workers also
		done := ctx.Done()
		iter := txn.NewIterator(badger.IteratorOptions{})
		prefix := appendKey(nil, prefixByteEvent, 0)
		defer iter.Close()
		for i, tr := range steps {
			if i == 0 {
				b.seekEvent(iter, tr.Start)
			}
			start, max, step := tr.Start.Unix(), tr.End.Unix(), int64(tr.Step)
			for start < max && iter.ValidForPrefix(prefix) {
				end := start + step
				n := 0
				for ; iter.Valid(); iter.Next() {
					key := iter.Item().Key()
					ts, ok := parseEventKey(key)
					if ok && start <= ts && ts < end {
						n++
					} else {
						break
					}
				}
				if n > 0 {
					select {
					case tasks <- &task{start, end, n}:
					case <-done:
						return
					}
				}
				start = end
			}

		}
		return
	})
	wg.Wait()
	close(ch)
	// Return first error
	err, _ := <-ch
	return err
}

func (b *badgerStore) squash(start, end int64, numKeys int) error {
	txn := b.NewTransaction(true)
	defer txn.Discard()
	opt := badger.DefaultIteratorOptions
	if opt.PrefetchSize < numKeys {
		opt.PrefetchSize = numKeys
	}
	iter := txn.NewIterator(opt)
	defer iter.Close()
	seek := appendKey(nil, prefixByteEvent, uint64(start))
	var value []byte

	for iter.Seek(seek); iter.Valid(); iter.Next() {
		item := iter.Item()
		key := item.Key()
		ts, ok := parseEventKey(key)
		if !ok || ts >= end {
			break
		}
		v, err := item.Value()
		if err != nil {
			return err
		}
		if ts > start {
			if err := txn.Delete(key); err != nil {
				return err
			}
		}
		if ts < start {
			panic("Invalid seek")
		}
		value = append(value, v...)
	}
	if len(value) > 0 {
		if err := txn.Set(seek, value); err != nil {
			return err
		}
		return txn.Commit(nil)
	}
	return nil
}
