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

func (a Aliases) Set(label string, aliases ...string) {
	for _, alias := range aliases {
		a[alias] = label
	}
}

var defaultAliases = NewAliases()

func Alias(s string) string {
	return defaultAliases.Alias(s)
}
func SetAlias(label string, aliases ...string) {
	defaultAliases.Set(label, aliases...)
}
