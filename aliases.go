package meter

type Aliases map[string]string

func NewAliases(input map[string][]string) Aliases {
	lookup := make(map[string]string)
	for w, aliases := range input {
		for _, alias := range aliases {
			lookup[alias] = w
		}
	}
	return Aliases(lookup)
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
	for i := 0; i < n; n += 2 {
		a[aliases[i]] = aliases[i+1]
	}
}
