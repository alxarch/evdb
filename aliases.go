package meter

import (
	"net/url"
)

type Aliases map[string]string

func AliasesFromValues(input url.Values) Aliases {
	lookup := make(map[string]string)
	if len(input) > 0 {
		for w, aliases := range input {
			lookup[w] = w
			for _, alias := range aliases {
				lookup[alias] = w
			}
		}
	}
	return Aliases(lookup)
}
func NewAliases() Aliases {
	return Aliases(make(map[string]string))
}

func (a Aliases) Alias(s string) string {
	if a != nil {
		if alias, ok := a[s]; ok {
			return alias
		}
	}
	return s
}

func (a Aliases) Set(aliases ...string) {
	n := len(aliases)
	n -= n % 2
	for i := 0; i < n; i += 2 {
		a[aliases[i]] = aliases[i+1]
	}
}
