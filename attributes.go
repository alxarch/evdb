package meter

type Labels []string

func (labels Labels) Map() map[string]string {
	m := make(map[string]string)
	for i := 0; i < len(labels); i += 2 {
		m[labels[i]] = labels[i+1]
	}
	return m
}
