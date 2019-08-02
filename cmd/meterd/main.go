package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/alxarch/evdb"
	_ "github.com/alxarch/evdb/evbadger"
	"github.com/alxarch/evdb/evhttp"
	_ "github.com/alxarch/evdb/evredis"
)

var (
	addr     = flag.String("addr", ":8080", "HTTP listen address")
	readOnly = flag.Bool("readonly", false, "Don't create store endpoints")
	debug    = flag.Bool("debug", false, "Debug logs")
	basePath = flag.String("basepath", "", "Basepath for URLs")
	dbURL    = flag.String("db", "badger:///var/lib/meterd", "Database configuration URL")
	logInfo  = log.New(os.Stdout, "[INFO] ", log.Ldate|log.Ltime)
	logError = log.New(os.Stderr, "[ERROR] ", log.Ldate|log.Ltime)
)

func main() {
	flag.Parse()
	events := flag.Args()
	db, err := evdb.Open(*dbURL, events...)
	if err != nil {
		logError.Fatal(err)
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
		logInfo.Println("Shutting down...")
		if err := db.Close(); err != nil {
			logError.Printf("Failed to close db: %s\n", err)
			os.Exit(1)
		}
		os.Exit(0)
	}()
	var w evdb.Store
	if !*readOnly {
		w = db

	}
	srv := http.Server{
		Addr:     *addr,
		ErrorLog: logError,
		Handler:  evhttp.DefaultMux(db, w),
	}
	if prefix := *basePath; prefix != "" {
		prefix = "/" + strings.Trim(prefix, "/")
		srv.Handler = http.StripPrefix(prefix, srv.Handler)
	}

	logInfo.Printf("Serving %s on %s...\n", *dbURL, srv.Addr)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		logError.Printf("Server failed: %s\n", err)
	}

}
