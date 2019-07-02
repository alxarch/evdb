package main

import (
	"flag"
	"context"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"strings"

	"github.com/alxarch/go-meter/v2/mdbbadger"
	"github.com/alxarch/go-meter/v2/mdbhttp"
	"github.com/dgraph-io/badger/v2"
)

var (
	dir      = flag.String("dir", "/var/lib/meterd", "Data dir")
	addr     = flag.String("addr", ":8080", "HTTP listen address")
	debug    = flag.Bool("debug", false, "Debug logs")
	basePath = flag.String("basepath", "", "Basepath for URLs")
)

func main() {
	flag.Parse()
	logs := newLogger("[meterd] ", *debug)
	events := flag.Args()
	options := badger.DefaultOptions
	options.Dir = *dir
	options.Logger = logs
	options.ValueDir = *dir
	db, err := badger.Open(options)
	if err != nil {
		logs.err.Fatalf(`Failed to open db: %s`, err)
	}
	defer db.Close()
	sigc := make(chan os.Signal)
	signal.Notify(sigc, syscall.SIGTERM)
	signal.Notify(sigc, syscall.SIGINT)
	ctx := context.Background()
	done := ctx.Done()
	go func() {
		select {
		case <- sigc:
		case <- done:
		}
		logs.Println("Shutting down...")
		if err := db.Close(); err != nil {
			logs.err.Printf("Failed to close db: %s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
	mdb, err := mdbbadger.Open(db, events...)
	if err != nil {
		logs.err.Fatalf("Failed to open db: %s\n", err)
	}
	srv := http.Server{
		Addr:     *addr,
		ErrorLog: logs.err,
		Handler:  mdbhttp.Handler(mdb, events...),
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
