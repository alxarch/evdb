package meter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"path"
	"sync"
	"time"

	"github.com/alxarch/go-meter/tcodec"
)

const (
	QueryParamEvent      = "event"
	QueryParamResolution = "res"
	QueryParamStart      = "start"
	QueryParamEnd        = "end"
	QueryParamGroup      = "group"
	QueryParamMode       = "mode"
)

func ParseQuery(q url.Values, tdec tcodec.TimeDecoder) (s QueryBuilder, err error) {
	eventNames := q[QueryParamEvent]
	delete(q, QueryParamEvent)
	if len(eventNames) == 0 {
		err = fmt.Errorf("Missing query.%s", QueryParamEvent)
		return
	}
	if _, ok := q[QueryParamResolution]; ok {
		s.Resolution = q.Get(QueryParamResolution)
		delete(q, QueryParamResolution)
	} else {
		err = fmt.Errorf("Missing query.%s", QueryParamResolution)
		return
	}
	if _, ok := q[QueryParamGroup]; ok {
		s.Group = q[QueryParamGroup]
		delete(q, QueryParamGroup)
	}

	if start, ok := q[QueryParamStart]; !ok {
		err = fmt.Errorf("Missing query.%s", QueryParamStart)
		return
	} else if s.Start, err = tdec.UnmarshalTime(start[0]); err != nil {
		err = fmt.Errorf("Invalid query.%s: %s", QueryParamStart, err)
		return
	}
	delete(q, QueryParamStart)
	if end, ok := q[QueryParamEnd]; !ok {
		err = fmt.Errorf("Missing query.%s", QueryParamEnd)
		return
	} else if s.End, err = tdec.UnmarshalTime(end[0]); err != nil {
		err = fmt.Errorf("Invalid query.%s: %s", QueryParamEnd, err)
		return
	}
	delete(q, QueryParamEnd)
	s.Query = q
	if now := time.Now(); s.End.After(now) {
		s.End = now
	}
	if s.Start.IsZero() || s.Start.After(s.End) {
		s.Start = s.End
	}
	switch q.Get(QueryParamMode) {
	case "exact":
		s.Mode = ModeExact
	case "values":
		s.Mode = ModeValues
	default:
		s.Mode = ModeScan
	}
	delete(q, QueryParamMode)
	s.Events = eventNames
	return

}

type Logger interface {
	Printf(format string, args ...interface{})
}

type Controller struct {
	DB *DB
	*Registry
	Logger        Logger
	TimeDecoder   tcodec.TimeDecoder
	FlushInterval time.Duration
	once          sync.Once
	closeCh       chan struct{}
	wg            sync.WaitGroup
}

func (c *Controller) Close() {
	if c.closeCh == nil {
		return
	}
	close(c.closeCh)
	c.wg.Wait()
}

func (c *Controller) Flush(t time.Time) {
	events := c.Registry.Events()
	errCh := make(chan error, len(events))
	c.wg.Add(1)
	defer c.wg.Done()
	for _, e := range events {
		c.wg.Add(1)
		go func(e *Event) {
			errCh <- c.DB.Gather(t, e)
			c.wg.Done()
		}(e)
	}
	c.wg.Wait()
	close(errCh)
	if c.Logger != nil {
		for _, e := range events {
			err, ok := <-errCh
			if !ok {
				break
			}
			if err != nil {
				c.Logger.Printf("Failed to sync event %s: %s", e.Describe().Name(), err)
			}
		}
	}

}
func (c *Controller) init() {
	if c.FlushInterval > 0 {
		c.closeCh = make(chan struct{})
		go c.runFlush(c.FlushInterval)
	}
}

func (c *Controller) runFlush(interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case t := <-tick.C:
			go c.Flush(t)
		case <-c.closeCh:
			return
		}
	}
}

func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()
		qb, err := ParseQuery(q, c.TimeDecoder)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		var output interface{}
		events := c.Registry
		if events == nil {
			events = defaultRegistry
		}
		queries := qb.Queries(events)
		results, _ := c.DB.Query(queries...)
		switch qb.Mode {
		case ModeValues:
			output = results.FrequencyMap()
		default:
			output = results
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.Encode(output)
	case http.MethodPost:
		defer r.Body.Close()
		check, eventName := path.Split(r.URL.Path)
		if check != "/" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		event := c.Registry.Get(eventName)
		if event == nil {
			http.NotFound(w, r)
			return
		}
		s := getSync()
		defer putSync(s)
		s.buf.Reset()
		_, err := s.buf.ReadFrom(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		s.snapshot = s.snapshot[:0]
		if err = json.Unmarshal(s.buf.Bytes(), &s.snapshot); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if c.FlushInterval > 0 {
			c.once.Do(c.init)
			event.Merge(s.snapshot)
		} else {
			if err := c.DB.gather(time.Now(), event.Describe(), s.snapshot); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		}

		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

var snapshotPool sync.Pool

func getSnapshot() Snapshot {
	if x := snapshotPool.Get(); x != nil {
		return x.(Snapshot)
	}
	return make([]Counter, 0, 64)
}
func putSnapshot(s Snapshot) {
	if s == nil {
		return
	}
	snapshotPool.Put(s[:0])
}

var syncPool sync.Pool

type syncBuffer struct {
	buf      bytes.Buffer
	snapshot Snapshot
}

func getSync() *syncBuffer {
	if x := syncPool.Get(); x != nil {
		return x.(*syncBuffer)
	}
	return new(syncBuffer)
}
func putSync(s *syncBuffer) {
	if s == nil {
		return
	}
	syncPool.Put(s)
}

type Client struct {
	URL string
	*http.Client
}

func (c *Client) Batch(logger *log.Logger, events ...*Event) {
	wg := new(sync.WaitGroup)
	for _, e := range events {
		wg.Add(1)
		go func(e *Event) {
			defer wg.Done()
			if err := c.Sync(e); err != nil {
				if logger != nil {
					logger.Printf("Failed to sync event %s: %s\n", e.Describe().Name(), err)
				}
			}
		}(e)
	}
	wg.Wait()
}

func (c *Client) Run(ctx context.Context, interval time.Duration, logger *log.Logger, events ...*Event) {
	if ctx == nil {
		ctx = context.Background()
	}
	tick := time.NewTicker(interval)
	pack := time.NewTicker(time.Hour)
	defer c.Batch(logger, events...)
	for {
		select {
		case <-ctx.Done():
			pack.Stop()
			tick.Stop()
			return
		case <-tick.C:
			c.Batch(logger, events...)
		case <-pack.C:
			for _, event := range events {
				event.Pack()
			}
		}
	}
}

func (c *Client) Sync(e *Event) error {
	desc := e.Describe()
	url := path.Join(c.URL, desc.Name())

	s := getSync()
	defer putSync(s)
	s.snapshot = e.Flush(s.snapshot[:0])
	if len(s.snapshot) == 0 {
		return nil
	}
	s.buf.Reset()
	enc := json.NewEncoder(&s.buf)
	if err := enc.Encode(s.snapshot); err != nil {
		return err
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Post(url, "application/json", &s.buf)
	if err != nil {
		e.Merge(s.snapshot)
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		e.Merge(s.snapshot)
		return fmt.Errorf("Failed to sync event %s to %s: %d %s", desc.Name(), url, res.StatusCode, res.Status)
	}
	return nil
}
