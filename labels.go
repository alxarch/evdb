package meter

type LabelValues map[string]string

func FieldLabels(field []string) LabelValues {
	n := len(field)
	n -= n % 2
	values := LabelValues(make(map[string]string, n/2))
	for i := 0; i < n; i += 2 {
		values[field[i]] = field[i+1]
	}
	return values
}

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

func (values LabelValues) Equal(other LabelValues) bool {

	if len(values) != len(other) {
		return false
	}
	if values == nil && other == nil {
		return true
	}
	for key, value := range values {
		if other[key] != value {
			return false
		}
	}
	return true
}
