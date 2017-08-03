package meter

import (
	"strings"
)

type Labels map[string]string

func (labels Labels) Pairs() []string {
	pairs := make([]string, 2*len(labels))
	n := 0
	for key, value := range labels {
		pairs[n] = key
		n++
		pairs[n] = value
		n++
	}
	return pairs
}

func Label(s string) string {
	return strings.Trim(s, " \t\r\n\v\f")
}

// func NormalizeLabels(labels ...string) (out []string) {
// 	unq := make(map[string]bool)
// 	for _, label := range labels {
// 		if _, ok := unq[label]; !ok {
// 			unq[label] = true
// 		}
// 	}
// 	for label, _ := range unq {
// 		out = append(out, label)
// 	}
// 	if len(out) > 0 {
// 		sort.Strings(out)
// 	}
// 	return
// }
