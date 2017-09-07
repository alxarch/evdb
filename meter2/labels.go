package meter2

type LabelValues map[string]string

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
