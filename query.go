package meter

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Query is a query for event results
type Query struct {
	TimeRange
	Match      Fields   `json:"match,omitempty"`
	Group      []string `json:"group,omitempty"`
	EmptyValue string   `json:"empty,omitempty"`
}

// SetValues sets query values from a URL query
func (q *Query) SetValues(values url.Values) {
	if step, ok := values["step"]; ok {
		if len(step) > 0 {
			q.Step, _ = time.ParseDuration(step[0])
		} else {
			q.Step = 0
		}
	} else {
		q.Step = -1
	}
	start, _ := strconv.ParseInt(values.Get("start"), 10, 64)
	q.Start = time.Unix(start, 0)
	end, _ := strconv.ParseInt(values.Get("end"), 10, 64)
	q.End = time.Unix(end, 0)
	match := q.Match[:0]
	for key, values := range values {
		if strings.HasPrefix(key, "match.") {
			label := strings.TrimPrefix(key, "match.")
			for _, value := range values {
				match = append(match, Field{
					Label: label,
					Value: value,
				})
			}
		}
	}
	sort.Stable(match)
	q.Match = match
	group, ok := values["group"]
	if ok && len(group) == 0 {
		group = make([]string, 0, len(q.Match))
		for i := range q.Match {
			m := &q.Match[i]
			group = appendDistinct(group, m.Label)
		}
	}
	q.Group = group
	q.EmptyValue = values.Get("empty")
}

// TimeRange is a range of time with a specific step
type TimeRange struct {
	Start time.Time     `json:"start"`
	End   time.Time     `json:"end"`
	Step  time.Duration `json:"step"`
}

type QueryRunner interface {
	RunQuery(ctx context.Context, q *Query, events ...string) (Results, error)
}

func QueryHandler(qr QueryRunner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		events := values["event"]
		q := Query{}
		q.SetValues(values)
		typ := ResultTypeFromString(values.Get("results"))
		if typ == TotalsResult {
			q.Step = -1
		}
		ctx := r.Context()
		results, err := qr.RunQuery(ctx, &q, events...)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var x interface{}
		switch typ {
		case TotalsResult:
			x = results.Totals()
		case FieldSummaryResult:
			x = results.FieldSummaries()
		case EventSummaryResult:
			x = results.EventSummaries(q.EmptyValue)
		default:
			x = results
		}
		enc := json.NewEncoder(w)
		w.Header().Set("Content-Type", "application/json")
		enc.Encode(x)
	}
}
