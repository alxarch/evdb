package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path"
	"strings"
	"syscall"

	"github.com/alxarch/evdb"
	"github.com/alxarch/evdb/evbadger"
	"github.com/alxarch/evdb/evhttp"
	"github.com/alxarch/evdb/evredis"
	"github.com/dgraph-io/badger/v2"
)

var (
	addr     = flag.String("addr", ":8080", "HTTP listen address")
	debug    = flag.Bool("debug", false, "Debug logs")
	basePath = flag.String("basepath", "", "Basepath for URLs")
	dbURL    = flag.String("db", "file:///var/lib/meterd", "Data dir")
	logs     *logger
)

func main() {
	flag.Parse()
	logs = newLogger("[meterd] ", *debug)
	events := flag.Args()
	db, err := open(*dbURL, events...)
	if err != nil {
		logs.err.Fatal(err)
	}
	defer db.Close()
	sigc := make(chan os.Signal)
	signal.Notify(sigc, syscall.SIGTERM)
	signal.Notify(sigc, syscall.SIGINT)
	ctx := context.Background()
	done := ctx.Done()
	go func() {
		select {
		case <-sigc:
		case <-done:
		}
		logs.Println("Shutting down...")
		if err := db.Close(); err != nil {
			logs.err.Printf("Failed to close db: %s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
	srv := http.Server{
		Addr:     *addr,
		ErrorLog: logs.err,
		Handler:  evhttp.DefaultMux(db, events...),
	}
	if prefix := *basePath; prefix != "" {
		prefix = "/" + strings.Trim(prefix, "/")
		srv.Handler = http.StripPrefix(prefix, srv.Handler)
	}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logs.err.Printf("Server failed: %s\n", err)
	}

}

type logger struct {
	*log.Logger
	err   *log.Logger
	debug *log.Logger
}

func newLogger(prefix string, debug bool) *logger {
	lo := logger{
		err:    log.New(os.Stderr, prefix, log.LstdFlags),
		Logger: log.New(os.Stdout, prefix, log.LstdFlags),
	}
	if debug {
		lo.debug = log.New(os.Stderr, "[DEBUG] "+prefix, log.LstdFlags)
	} else {
		lo.debug = log.New(ioutil.Discard, "[DEBUG] "+prefix, log.LstdFlags)
	}
	return &lo
}
func (log *logger) Debugf(format string, args ...interface{}) {
	log.debug.Printf(format, args...)
}
func (log *logger) Errorf(format string, args ...interface{}) {
	log.err.Printf(format, args...)
}
func (log *logger) Warningf(format string, args ...interface{}) {
	log.err.Printf(format, args...)
}
func (log *logger) Infof(format string, args ...interface{}) {
	log.Logger.Printf(format, args...)
}

func open(dbURL string, events ...string) (evdb.DB, error) {
	u, err := url.Parse(dbURL)
	if err != nil {
		return nil, err
	}
	switch u.Scheme {
	case "redis":
		opts, err := evredis.ParseURL(dbURL)
		if err != nil {
			return nil, err
		}
		db, err := evredis.Open(opts, events...)
		if err != nil {
			return nil, err
		}
		return db, nil
	case "file", "badger":
		options := badger.DefaultOptions
		options.Dir = path.Join(u.Host, u.Path)
		options.Logger = logs
		options.ValueDir = options.Dir
		b, err := badger.Open(options)
		if err != nil {
			logs.err.Fatalf(`Failed to open db: %s`, err)
		}
		db, err := evbadger.Open(b, events...)
		if err != nil {
			logs.err.Fatalf("Failed to open db: %s\n", err)
		}
		return db, nil
	default:
		return nil, fmt.Errorf(`Invalid db URL: %s`, dbURL)
	}

}
