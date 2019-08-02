package evbadger

import (
	"log"
	"os"

	"github.com/dgraph-io/badger/v2"
)

type debugLogger struct {
	badger.Logger
	debug *log.Logger
}
type logger struct {
	info *log.Logger
	warn *log.Logger
	err  *log.Logger
}

func (lo *logger) Infof(format string, args ...interface{}) {
	lo.info.Printf(format, args...)
}
func (lo *logger) Warningf(format string, args ...interface{}) {
	lo.warn.Printf(format, args...)
}
func (lo *logger) Errorf(format string, args ...interface{}) {
	lo.err.Printf(format, args...)
}
func (*logger) Debugf(string, ...interface{}) {
}

func (lo *debugLogger) Debugf(format string, args ...interface{}) {
	lo.debug.Printf(format, args...)
}

const logFlags = log.Ldate | log.Ltime

func newLogger() badger.Logger {
	lo := logger{
		info: log.New(os.Stdout, "[INFO] ", logFlags),
		warn: log.New(os.Stderr, "[WARN] ", logFlags),
		err:  log.New(os.Stderr, "[ERROR] ", logFlags),
	}
	return &lo
}
func newDebugLogger() badger.Logger {
	lo := debugLogger{
		Logger: newLogger(),
		debug:  log.New(os.Stderr, "[DEBUG] ", logFlags),
	}
	return &lo
}
