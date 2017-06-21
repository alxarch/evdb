package meter

import (
	"sort"
	"strings"
)

type Labels []string

func (labels Labels) Map() map[string]string {
	n := len(labels)
	n = n - (n % 2)
	labels = labels[:n]
	m := make(map[string]string)
	if n > 0 {
		for i := 0; i < n; i += 2 {
			if k, v := labels[i], labels[i+1]; v != "" && v != "*" {
				m[k] = v
			}
		}
	}
	return m
}

func (labels Labels) Set(pairs ...string) Labels {
	n := len(pairs)
	n = n - (n % 2)
	if n > 0 {
		labels = append(labels, pairs[:n]...)
	}
	return labels

}

func Label(s string) string {
	return strings.Trim(s, " \t\r\n\v\f")
}

func NormalizeLabels(labels ...string) (out []string) {
	unq := make(map[string]bool)
	for _, label := range labels {
		if _, ok := unq[label]; !ok {
			unq[label] = true
		}
	}
	for label, _ := range unq {
		out = append(out, label)
	}
	if len(out) > 0 {
		sort.Strings(out)
	}
	return
}
