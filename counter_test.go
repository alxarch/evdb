package meter

import (
	"testing"
)

func TestCounter_UnmarshalJSON(t *testing.T) {
	type fields struct {
		n      int64
		values []string
	}
	type args struct {
		data []byte
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"normal", fields{42, []string{"foo", "bar", "baz"}}, args{[]byte(`[42,["foo","bar","baz"]]`)}, false},
		{"null", fields{0, nil}, args{[]byte(`null`)}, false},
		{"empty array", fields{0, nil}, args{[]byte(`[]`)}, false},
		{"no-labels", fields{0, nil}, args{[]byte(`[42]`)}, false},
		{"errjson", fields{0, nil}, args{[]byte(`{}`)}, true},
		{"errn", fields{0, nil}, args{[]byte(`["foo",["bar"]]`)}, true},
		{"empty", fields{0, nil}, args{[]byte(`[]`)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Counter{}
			if err := c.UnmarshalJSON(tt.args.data); (err != nil) != tt.wantErr {
				t.Errorf("Counter.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if c.Count != tt.fields.n {
				t.Errorf("Counter.UnmarshalJSON() n = %d, want %d", c.Count, tt.fields.n)
			}
			if !c.Match(tt.fields.values) {
				t.Errorf("Counter.UnmarshalJSON() v = %v, want %v", c.Values, tt.fields.values)
			}
		})
	}
}

func TestCounter_AppendJSON(t *testing.T) {
	type fields struct {
		n      int64
		values []string
	}
	tests := []struct {
		name string
		c    Counter
		want string
	}{
		{"normal", Counter{42, []string{"foo", "bar", "baz"}}, `[42,["foo","bar","baz"]]`},
		{"empty", Counter{}, `[0,[]]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.c.AppendJSON(nil); string(got) != tt.want {
				t.Errorf("Counter.AppendJSON() = %s, want %s", got, tt.want)
			}
			if got, _ := tt.c.MarshalJSON(); string(got) != tt.want {
				t.Errorf("Counter.MarshalJSON() = %s, want %s", got, tt.want)
			}
		})
	}
}

func TestSnapshot_AppendJSON(t *testing.T) {
	tests := []struct {
		name string
		s    Snapshot
		want string
	}{
		{"empty", Snapshot{}, "[]"},
		{"single", Snapshot{Counter{42, []string{"foo", "bar", "baz"}}}, `[[42,["foo","bar","baz"]]]`},
		{"multi", Snapshot{
			Counter{42, []string{"foo", "bar", "baz"}},
			Counter{42, []string{"foo", "bar"}},
		}, `[[42,["foo","bar","baz"]],[42,["foo","bar"]]]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.s.AppendJSON(nil); string(got) != tt.want {
				t.Errorf("Snapshot.AppendJSON() = %s, want %s", got, tt.want)
			}
			if got, _ := tt.s.MarshalJSON(); string(got) != tt.want {
				t.Errorf("Snapshot.MarshalJSON() = %s, want %s", got, tt.want)
			}
		})
	}
}
