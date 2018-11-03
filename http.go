package meter

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/dgraph-io/badger"
)

// const (
// 	QueryParamEvent      = "event"
// 	QueryParamResolution = "res"
// 	QueryParamStart      = "start"
// 	QueryParamEnd        = "end"
// 	QueryParamGroup      = "group"
// 	QueryParamMode       = "mode"
// )

// func ParseQuery(q url.Values, tdec tcodec.TimeDecoder) (s QueryBuilder, err error) {
// 	eventNames := q[QueryParamEvent]
// 	delete(q, QueryParamEvent)
// 	if len(eventNames) == 0 {
// 		err = fmt.Errorf("Missing query.%s", QueryParamEvent)
// 		return
// 	}
// 	if _, ok := q[QueryParamResolution]; ok {
// 		s.Resolution = q.Get(QueryParamResolution)
// 		delete(q, QueryParamResolution)
// 	} else {
// 		err = fmt.Errorf("Missing query.%s", QueryParamResolution)
// 		return
// 	}
// 	if _, ok := q[QueryParamGroup]; ok {
// 		s.Group = q[QueryParamGroup]
// 		delete(q, QueryParamGroup)
// 	}

// 	if start, ok := q[QueryParamStart]; !ok {
// 		err = fmt.Errorf("Missing query.%s", QueryParamStart)
// 		return
// 	} else if s.Start, err = tdec.UnmarshalTime(start[0]); err != nil {
// 		err = fmt.Errorf("Invalid query.%s: %s", QueryParamStart, err)
// 		return
// 	}
// 	delete(q, QueryParamStart)
// 	if end, ok := q[QueryParamEnd]; !ok {
// 		err = fmt.Errorf("Missing query.%s", QueryParamEnd)
// 		return
// 	} else if s.End, err = tdec.UnmarshalTime(end[0]); err != nil {
// 		err = fmt.Errorf("Invalid query.%s: %s", QueryParamEnd, err)
// 		return
// 	}
// 	delete(q, QueryParamEnd)
// 	s.Query = q
// 	if now := time.Now(); s.End.After(now) {
// 		s.End = now
// 	}
// 	if s.Start.IsZero() || s.Start.After(s.End) {
// 		s.Start = s.End
// 	}
// 	switch q.Get(QueryParamMode) {
// 	case "exact":
// 		s.Mode = ModeExact
// 	case "values":
// 		s.Mode = ModeValues
// 	default:
// 		s.Mode = ModeScan
// 	}
// 	delete(q, QueryParamMode)
// 	s.Events = eventNames
// 	return

// }

func DumpKeysHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		db.View(func(txn *badger.Txn) error {
			iter := txn.NewIterator(badger.IteratorOptions{
				PrefetchValues: false,
			})
			defer iter.Close()
			for iter.Seek(nil); iter.Valid(); iter.Next() {
				item := iter.Item()
				key := item.Key()
				id := binary.BigEndian.Uint64(key[len(key)-8:])
				switch key[0] {
				case 'v':
					v, _ := item.Value()
					fields := FieldsFromString(string(v))
					fmt.Fprintf(w, "v %q %08x %v\n", key[2:len(key)-8], id, fields)
				case 'e':
					fmt.Fprintf(w, "e %q@%d\n", key[2:len(key)-8], id)

				}
			}
			return nil
		})
	}

}
func Handler(db *DB) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/scan", ScanHandler(db))
	mux.HandleFunc("/summary", SummaryHandler(db))
	mux.HandleFunc("/", StoreHandler(db))
	mux.HandleFunc("/keys", DumpKeysHandler(db))
	return mux
}

type StoreRequest struct {
	Time     time.Time `json:"time,omitempty"`
	Event    string    `json:"event"`
	Labels   []string  `json:"labels"`
	Counters Snapshot  `json:"counters"`
}

func (r *StoreRequest) Reset() {
	for i := range r.Counters {
		r.Counters[i] = Counter{}
	}
	*r = StoreRequest{
		Labels:   r.Labels[:0],
		Counters: r.Counters[:0],
	}
}

func StoreHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		defer r.Body.Close()
		s := getStoreBuffer()
		defer putStoreBuffer(s)
		_, err := s.Buffer.ReadFrom(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if err := json.Unmarshal(s.Buffer.Bytes(), &s.StoreRequest); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if s.Time.IsZero() {
			s.Time = time.Now()

		}
		if err := db.Store(s.Time, s.Event, s.Labels, s.Counters); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}
}

func SummaryHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := new(SummaryScan)
		q.SetValues(r.URL.Query())
		index, err := db.scanValues(q.Event, q.Matcher())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		err = db.scanEvents(q.Event, q.Start, q.End, 0, index, q)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		enc := json.NewEncoder(w)
		w.Header().Set("Content-Type", "application/json")
		if err := enc.Encode(q); err != nil {
			log.Println(err)
		}
	}
}
func ScanHandler(db *DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		q := new(TimeSeriesScan)
		q.SetValues(r.URL.Query())
		index, err := db.scanValues(q.Event, q.Matcher())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		err = db.scanEvents(q.Event, q.Start, q.End, q.Step, index, q)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		enc := json.NewEncoder(w)
		w.Header().Set("Content-Type", "application/json")
		enc.Encode(q)
	}
}

type HTTPClient struct {
	URL string
	*http.Client
}

func (c *HTTPClient) Batch(logger *log.Logger, events ...*Event) {
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

func (c *HTTPClient) Run(ctx context.Context, interval time.Duration, logger *log.Logger, events ...*Event) {
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

var storeBufferPool sync.Pool

type storeBuffer struct {
	StoreRequest
	bytes.Buffer
}

func putStoreBuffer(s *storeBuffer) {
	if s != nil {
		s.Buffer.Reset()
		s.StoreRequest.Reset()
		storeBufferPool.Put(s)
	}
}
func getStoreBuffer() *storeBuffer {
	if x := storeBufferPool.Get(); x != nil {
		return x.(*storeBuffer)
	}
	return new(storeBuffer)
}

func (c *HTTPClient) Sync(e *Event) error {
	desc := e.Describe()

	s := getStoreBuffer()
	defer putStoreBuffer(s)
	s.Counters = e.Flush(s.Counters)
	if desc.Type() == MetricTypeIncrement {
		s.Counters = s.Counters.FilterZero()
	}
	if len(s.Counters) == 0 {
		return nil
	}
	s.Time = time.Now()
	s.Event = desc.Name()
	s.Labels = append(s.Labels[:0], desc.Labels()...)
	enc := json.NewEncoder(&s.Buffer)
	if err := enc.Encode(&s.StoreRequest); err != nil {
		return err
	}
	client := c.Client
	if client == nil {
		client = http.DefaultClient
	}
	res, err := client.Post(c.URL, "application/json", &s.Buffer)
	if err != nil {
		e.Merge(s.Counters)
		return err
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		e.Merge(s.Counters)
		return fmt.Errorf("Failed to sync event %s to %s: %d %s", desc.Name(), c.URL, res.StatusCode, res.Status)
	}
	return nil
}
