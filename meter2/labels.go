package meter2

type LabelValues map[string]string

// func (labels Labels) WithAliases(aliases Aliases) Labels {
// 	aliased := make(map[string]string, len(labels))
// 	for key, value := range labels {
// 		aliased[aliases.Alias(key)] = value
// 	}
// 	return aliased
// }

// func (labels Labels) Pairs() []string {
// 	if labels == nil {
// 		return nil
// 	}
// 	pairs := make([]string, 2*len(labels))
// 	n := 0
// 	for key, value := range labels {
// 		pairs[n] = key
// 		n++
// 		pairs[n] = value
// 		n++
// 	}
// 	return pairs
// }

func (values LabelValues) Values(labels []string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(labels))
	for i, label := range labels {
		out[i] = values[label]
	}
	return out

}
