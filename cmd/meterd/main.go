package main

import (
	"flag"
	"log"
	"net/http"
	"os"
	"path"
	"time"

	meter "github.com/alxarch/go-meter"
	"github.com/dgraph-io/badger"
)

var (
	dataDir    = flag.String("dir", "", "Data dir")
	addr       = flag.String("address", ":8080", "HTTP Listen address")
	gcInterval = flag.Duration("gc-interval", 5*time.Minute, "Badger values GC interval")
	ttl        = flag.Duration("ttl", 0, "Time series TTL")
	minStep    = flag.Duration("min-step", time.Second, "Minimum step for results")
)

func main() {
	flag.Parse()
	if *dataDir == "" {
		*dataDir = path.Join(os.TempDir(), "meterd")
		// os.MkdirAll(path.Join(*dataDir, "values"), os.ModeAppend)
	}
	options := badger.DefaultOptions
	options.Dir = *dataDir
	options.ValueDir = path.Join(*dataDir, "values")
	db, err := badger.Open(options)

	if err != nil {
		log.Fatal("Failed to open db", err)
	}
	defer db.Close()
	mdb := meter.DB{
		TTL:     *ttl,
		MinStep: *minStep,
		DB:      db,
	}
	if err := http.ListenAndServe(*addr, meter.Handler(&mdb)); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
