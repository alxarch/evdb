package evql_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	db "github.com/alxarch/evdb"
	"github.com/alxarch/evdb/evql"
)

func TestParser_Reset(t *testing.T) {
	tests := []struct {
		query   string
		wantErr bool
	}{
		{`foo`, false},
		{`foo{bar: baz}`, false},
		{`foo{bar: "baz"}`, false},
		{`foo{bar: baz|"foo-bar"|goo}`, false},
		{`foo{bar: baz, bar: foo}`, false},
		{`foo[-1:h]; *WHERE{foo: bar}; {*WHERE{bar: baz}; bar}`, false},
		{`*GROUP{foo}; foo/foo[-1:h]`, false},
		{`*SELECT{foo} ; *GROUP{bar}`, false},
		{`*GROUP{foo}; foo/foo[-1:h] + 1`, false},
		{`*GROUP{foo}; foo + 1.0`, false},
		{`*GROUP{foo}; foo + !vAVG{bar[-1:h]}`, false},
		{`foo{bar: !regexp(baz)}`, false},
		{`foo{bar: baz|foo}; *BY{foo}; *OFFSET[1:h]`, false},
		{`!avg{foo{bar: baz}}; *GROUP{foo}`, false},
		{`!zipavg{foo{bar: baz}, !avg{bar[-1:d]}}; *BY{foo}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			_, err := evql.Parse(tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Parser.Reset() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParser(t *testing.T) {
	now := time.Now()
	tr := db.TimeRange{
		Start: now.Add(-1 * time.Hour),
		End:   now,
		Step:  time.Hour,
	}
	all := db.Results{
		{
			Event:     "foo",
			Fields:    db.Fields{{"color", "red"}, {"size", "s"}},
			TimeRange: tr,
			Data:      db.BlankData(&tr, 8),
		},
		{
			Event:     "foo",
			Fields:    db.Fields{{"color", "red"}, {"size", "m"}},
			TimeRange: tr,
			Data:      db.BlankData(&tr, 9),
		},
		{
			Event:     "bar",
			Fields:    db.Fields{{"color", "blue"}, {"size", "s"}},
			TimeRange: tr,
			Data:      db.BlankData(&tr, 10),
		},
		{
			Event:     "bar",
			Fields:    db.Fields{{"color", "blue"}, {"size", "xl"}},
			TimeRange: tr,
			Data:      db.BlankData(&tr, 11),
		},
		{
			Event:     "baz",
			Fields:    db.Fields{{"color", "blue"}, {"size", "xl"}, {"brand", "zag"}},
			TimeRange: tr,
			Data:      db.BlankData(&tr, 12),
		},
		{
			Event:     "baz",
			Fields:    db.Fields{{"color", "red"}, {"size", "s"}, {"brand", "zag"}},
			TimeRange: tr,
			Data:      db.BlankData(&tr, 15),
		},
	}
	scanner := db.NewScanner(all)
	ex := evql.NewExecer(scanner)
	tests := []struct {
		query       string
		tr          db.TimeRange
		wantErr     bool
		wantResults []interface{}
	}{
		{`foo`, tr, false, []interface{}{&all[0], &all[1]}},
		{`foo{color: red}`, tr, false, []interface{}{&all[0], &all[1]}},
		{`foo{color: blue}`, tr, false, nil},
		{`foo{size: s|m}`, tr, false, []interface{}{&all[0], &all[1]}},
		{`foo{size: s}`, tr, false, []interface{}{&all[0]}},
		{`foo + bar; *BY{size}; *WHERE{color:blue|red}`, tr, false, []interface{}{
			&db.Result{
				Event:     "foo + bar",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "s"}},
				Data:      db.BlankData(&tr, 18),
			},
			&db.Result{
				Event:     "foo + bar",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "xl"}},
				Data:      db.BlankData(&tr, 11),
			},
			&db.Result{
				Event:     "foo + bar",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "m"}},
				Data:      db.BlankData(&tr, 9),
			},
		}},
		{`!ZIPAVG{foo, bar}; *BY{size}`, tr, false, []interface{}{
			&db.Result{
				Event:     "!ZIPAVG{foo, bar}",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "s"}},
				Data:      db.BlankData(&tr, 9),
			},
			&db.Result{
				Event:     "!ZIPAVG{foo, bar}",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "xl"}},
				Data:      db.BlankData(&tr, 11/2.0),
			},
			&db.Result{
				Event:     "!ZIPAVG{foo, bar}",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "m"}},
				Data:      db.BlankData(&tr, 9/2.0),
			},
		}},
		{`!avg{foo}; *BY{size}`, tr, false, []interface{}{
			&db.Result{
				Event:     "!avg{foo}",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "s"}},
				Data:      db.BlankData(&tr, 8),
			},
			&db.Result{
				Event:     "!avg{foo}",
				TimeRange: tr,
				Fields:    db.Fields{{"size", "m"}},
				Data:      db.BlankData(&tr, 9),
			},
		}},
		// {`foo{bar: baz}`, false},
		// {`foo{bar: "baz"}`, false},
		// {`foo{bar: baz|"foo-bar"|goo}`, false},
		// {`foo{bar: baz, bar: foo}`, false},
		// {`foo[-1:h]`, false},
		// {`*GROUP{foo}; foo/foo[-1:h]`, false},
		// {`foo{bar: !regexp(baz)}`, false},
		// {`foo{bar: baz|foo}; *BY{foo}; *OFFSET[1:h]`, false},
		// {`!avg{foo{bar: baz}}; *GROUP{foo}`, false},
		// {`!zipavg{foo{bar: baz}, bar[-1:d]}; *BY{foo}`, false},
	}
	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.query, func(t *testing.T) {
			results, err := ex.Exec(ctx, tt.tr, tt.query)
			if (err != nil) != tt.wantErr {
				t.Errorf("Query error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(results) != len(tt.wantResults) {
				t.Errorf("Query results invalid %v != %v", results, tt.wantResults)
				return
			}
			for i := range results {
				if !reflect.DeepEqual(results[i], tt.wantResults[i]) {
					t.Errorf("Query result[%d] invalid %v != %v", i, results[i], tt.wantResults[i])

				}

			}
		})
	}

}
