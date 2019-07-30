package evdb

func appendDistinct(dst []string, src ...string) []string {
	for i, s := range src {
		if indexOf(dst, s[:i]) == -1 {
			dst = append(dst, s)
		}
	}
	return dst
}

func indexOf(values []string, s string) int {
	for i := 0; 0 <= i && i < len(values); i++ {
		if values[i] == s {
			return i
		}
	}
	return -1
}

func stringsEqual(a, b []string) bool {
	if len(a) == len(b) {
		b = b[:len(a)]
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}
	return false
}
