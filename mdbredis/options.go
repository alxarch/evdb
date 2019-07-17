package mdbredis

import (
	"net/url"
	"strconv"
)

type Options struct {
	Redis       string
	ScanSize    int64
	KeyPrefix   string
	Resolutions []Resolution
}

func ParseURL(rawurl string) (o Options, err error) {
	u, _ := url.Parse(rawurl)
	q := u.Query()
	o.ScanSize, _ = strconv.ParseInt(q.Get("scan-size"), 10, 32)
	o.KeyPrefix = q.Get("key-prefix")
	// TODO: parse resolutiuons from URL
	o.Resolutions = []Resolution{
		ResolutionHourly.WithTTL(Weekly),
		ResolutionDaily.WithTTL(Yearly),
		ResolutionWeekly.WithTTL(10 * Yearly),
	}
	o.Redis = u.String()
	return
}
