package meter

type Counter struct {
	Count  int64    `json:"n"`
	Values []string `json:"v"`
}

type Snapshot []Counter

func (s Snapshot) FilterZero() Snapshot {
	j := 0
	for i := range s {
		c := &s[i]
		if c.Count == 0 {
			continue
		}
		s[j] = *c
		j++
	}
	return s[:j]
}

func (c *Counter) Match(values []string) bool {
	if len(c.Values) == len(values) {
		values = values[:len(c.Values)]
		for i := range c.Values {
			if c.Values[i] == values[i] {
				continue
			}
			return false
		}
		return true
	}
	return false
}

// func (c *Counter) UnmarshalJSON(data []byte) error {
// 	data = bytes.TrimSpace(data)
// 	if len(data) > 0 {
// 		if data[0] == '[' && data[len(data)-1] == ']' {
// 			data = bytes.TrimSpace(data[1 : len(data)-1])
// 			pos := bytes.IndexByte(data, ',')
// 			if pos == -1 {
// 				return nil
// 			}
// 			n, err := strconv.ParseInt(string(data[:pos]), 10, 64)
// 			if err != nil {
// 				return err
// 			}
// 			c.Count = n
// 			return json.Unmarshal(data[pos+1:], &c.Values)
// 		}
// 		if data[0] == 'n' {
// 			if string(data) == "null" {
// 				return nil
// 			}
// 		}
// 	}
// 	return errors.New("Invalid JSON data")

// }

// func (s Snapshot) MarshalJSON() ([]byte, error) {
// 	return s.AppendJSON(nil), nil
// }

// func (c *Counter) MarshalJSON() ([]byte, error) {
// 	return c.AppendJSON(nil), nil
// }

// func (c *Counter) AppendJSON(dst []byte) []byte {
// 	dst = append(dst, '[')
// 	dst = strconv.AppendInt(dst, c.Count, 10)
// 	dst = append(dst, ',', '[')
// 	for i, v := range c.Values {
// 		if i > 0 {
// 			dst = append(dst, ',')
// 		}
// 		dst = append(dst, '"')
// 		dst = append(dst, v...)
// 		dst = append(dst, '"')
// 	}
// 	dst = append(dst, ']', ']')
// 	return dst
// }

// func (s Snapshot) AppendJSON(dst []byte) []byte {
// 	dst = append(dst, '[')
// 	for i := range s {
// 		if i > 0 {
// 			dst = append(dst, ',')
// 		}
// 		c := &s[i]
// 		dst = c.AppendJSON(dst)
// 	}
// 	dst = append(dst, ']')
// 	return dst
// }
