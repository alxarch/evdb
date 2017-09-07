package meter2

import (
	"regexp"
	"strconv"
	"strings"
	"time"
)

type SummaryQuery struct {
	Time       time.Time
	Event      string
	Labels     LabelValues
	Group      string
	Resolution Resolution
}

type Summary map[string]int64

func (s Summary) Add(other Summary) {
	for key, count := range other {
		s[key] += count
	}
}
func (db *DB) ParseField(field string) LabelValues {
	if field == "*" {
		return LabelValues{}
	}

	// Strip FieldTerminator
	if n := len(field) - 1; n != -1 && field[n] == FieldTerminator {
		field = field[:n]
	}

	tmp := strings.Split(field, string(LabelSeparator))
	n := len(tmp)
	n -= n % 2
	labels := LabelValues(make(map[string]string, n/2))
	for i := 0; i < n; i += 2 {
		labels[tmp[i]] = tmp[i+1]
	}
	return labels
}

var quoteMeta = regexp.QuoteMeta

func (db *DB) MatchField(labels []string, group string, values LabelValues) (f string) {
	b := bget()
	n := 0
	for _, label := range labels {
		if label == group {
			if n > 0 {
				b.WriteByte(LabelSeparator)
			}
			b.WriteString(quoteMeta(label))
			b.WriteByte(LabelSeparator)
			b.WriteByte('*')
			n++
		} else if v, ok := values[label]; ok && v != "" && v != "*" {
			if n > 0 {
				b.WriteByte(LabelSeparator)
			}
			b.WriteString(quoteMeta(label))
			b.WriteByte(LabelSeparator)
			b.WriteString(quoteMeta(v))
			n++
		}
	}
	if n == 0 {
		b.WriteString("\\*")
	}
	b.WriteByte(FieldTerminator)
	f = b.String()
	bput(b)
	return
}

func (db *DB) SummaryScan(q SummaryQuery) (sum Summary, err error) {
	event := db.registry.Get(q.Event)
	if event == nil {
		return nil, ErrUnregisteredEvent
	}
	group := q.Group
	// if db.Aliases != nil {
	// 	group = db.Aliases.Alias(group)
	// }
	labels := q.Labels
	// if db.Aliases != nil {
	// 	labels = labels.WithAliases(db.Aliases)
	// }
	metric := event.WithLabels(labels)
	desc := metric.Describe()
	if err = desc.Describe().Error(); err != nil {
		return
	}
	if !desc.HasLabel(group) {
		return nil, ErrInvalidEventLabel
	}
	match := db.MatchField(desc.Labels(), group, labels)
	name := desc.Name()
	cursor := uint64(0)
	key := db.Key(q.Resolution, name, q.Time)
	fields := []string{}
	for {
		reply := db.Redis.HScan(key, cursor, match, -1)
		var keys []string
		if keys, cursor, err = reply.Result(); err != nil {
			return
		}
		fields = append(fields, keys...)
		if cursor == 0 {
			break
		}
	}
	var values []interface{}
	if values, err = db.Redis.HMGet(key, fields...).Result(); err != nil {
		return
	}
	sum = Summary(make(map[string]int64, len(values)))
	for i, field := range fields {
		if key, ok := db.ParseField(field)[group]; ok {
			switch value := values[i].(type) {
			case string:
				if n, e := strconv.ParseInt(value, 10, 64); e == nil {
					sum[key] += n
				}
			}
		}
	}
	return

}
