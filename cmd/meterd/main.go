package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"syscall"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	badger "github.com/dgraph-io/badger/v2"
)

var (
	dataDir = flag.String("dir", "", "Data dir")
	addr    = flag.String("address", ":8080", "HTTP Listen address")
)

func main() {
	flag.Parse()
	if *dataDir == "" {
		*dataDir = path.Join(os.TempDir(), "meterd")
		if err := os.MkdirAll(*dataDir, os.ModePerm); err != nil {
			log.Fatal("Failed to create tmp data dir", err)
		}
	}

	options := badger.DefaultOptions
	options.Truncate = true
	options.Dir = *dataDir
	options.ValueDir = *dataDir
	db, err := badger.Open(options)
	if err != nil {
		log.Fatal("Failed to open badger DB", err)
	}
	defer db.Close()
	events, err := meter.Open(db, flag.Args()...)
	if err != nil {
		log.Fatal("Failed to open event db", err)
	}
	ctx := context.Background()
	for event := range events {
		tick := time.NewTicker(time.Hour)
		db := events[event]
		run := func(tm time.Time) {
			if err := db.Compaction(tm); err != nil {
				log.Println("Compaction failed", event, err)
			}
			if err := db.RunValueLogGC(0.5); err != nil {
				if err != badger.ErrNoRewrite {
					log.Println("Value log GC failed", event, err)
				}
			}
		}
		go func() {
			run(time.Now())
			defer db.Close()
			defer tick.Stop()
			for {
				select {
				case tm := <-tick.C:
					run(tm)
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	q := meter.ScanQueryRunner(events)
	queryHandler := meter.QueryHandler(q)
	storeHandler := meter.StoreHandler(events)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		for _, e := range events {
			meter.DumpKeys(e.DB, w)
			return
		}
	})
	mux.HandleFunc("/events", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			storeHandler(w, r)
		case http.MethodGet:
			queryHandler(w, r)
		default:
			http.Error(w, http.StatusText(http.StatusMethodNotAllowed), http.StatusMethodNotAllowed)
		}
	})

	s := http.Server{
		Addr:              *addr,
		Handler:           mux,
		MaxHeaderBytes:    4096,
		ReadHeaderTimeout: 5 * time.Second,
	}
	// Graceful shutdown
	done := make(chan struct{})
	go func() {
		defer close(done)
		sigint := make(chan os.Signal, 1)
		signal.Notify(sigint, os.Interrupt)
		signal.Notify(sigint, syscall.SIGTERM)
		<-sigint
		s.Close()
	}()
	// http.Handle("/events/", http.StripPrefix("/events", meter.Handler(events)))
	log.Println("Listening on", *addr)
	if err := s.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
	log.Println("Server closed")
	<-done
}
