package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"
	"time"

	meter "github.com/alxarch/go-meter/v2"
	badger "github.com/alxarch/go-meter/v2/db/badger"
)

var (
	dataDir    = flag.String("dir", "", "Storage dir")
	eventNames = flag.String("events", "", "Event names")
	addr       = flag.String("address", ":8080", "HTTP Listen address")
)

func splitEvents(v string) []string {
	values := strings.Split(v, ",")
	for i, v := range values {
		values[i] = strings.TrimSpace(v)
	}
	return values
}
func main() {
	flag.Parse()
	if *dataDir == "" {
		*dataDir = path.Join(os.TempDir(), "meterd")
		if err := os.MkdirAll(*dataDir, os.ModePerm); err != nil {
			log.Fatal("Failed to create tmp data dir", err)
		}
	}
	storageURL := fmt.Sprintf(`badger://%s?truncate=true`, *dataDir)
	db, err := meter.Open(storageURL, splitEvents(*eventNames)...)
	if err != nil {
		log.Fatal("Failed to open ")
	}

	defer db.Close()
	ctx := context.Background()
	go func() {
		tick := time.NewTicker(time.Hour)
		run := func(tm time.Time) {
			if err := db.(badger.DB).Compaction(tm); err != nil {
				log.Println("Compaction failed", err)
			}
		}
		run(time.Now())
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
	queryHandler := meter.QueryHandler(db)
	storeHandler := meter.StoreHandler(db)
	mux := http.NewServeMux()
	mux.HandleFunc("/debug", func(w http.ResponseWriter, r *http.Request) {
		events, _ := db.(badger.DB)
		for _, e := range events {
			badger.DumpKeys(e.DB, w)
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
