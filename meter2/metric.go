package meter2

type Metric interface {
	Values() []string
	Count() int64
	Add(n int64) int64
	Set(n int64) int64
	Descriptor
}

// func AppendValueField(values []string, n int, w *bytes.Buffer) {
// 	if w == nil {
// 		return
// 	}
// 	if n < 0 || n > len(values) {
// 		n = len(values)
// 	}
// 	for i := 0; i < n; i++ {
// 		if i > 0 {
// 			w.WriteByte(LabelSeparator)
// 		}
// 		w.WriteString(values[i])
// 	}
// }
//
// func AppendValueFieldLabels(values map[string]string, labels []string, w *bytes.Buffer) {
// 	if w == nil {
// 		return
// 	}
// 	n := len(labels)
// 	for i := 0; i < n; i++ {
// 		if i > 0 {
// 			w.WriteByte(LabelSeparator)
// 		}
// 		w.WriteString(values[labels[i]])
// 	}
// }
//
// func SplitValueFieldN(field string, n int) (values []string) {
// 	if n == 0 {
// 		return
// 	}
// 	if n < 0 {
// 		n = 1
// 		for i := 0; i < len(field); i++ {
// 			if field[i] == LabelSeparator {
// 				n++
// 			}
// 		}
// 	}
// 	values = make([]string, n)
// 	i := 0
// 	for i < n && len(field) > 0 {
// 		if j := strings.IndexByte(field, LabelSeparator); j == -1 {
// 			values[i] = field
// 			i++
// 			break
// 		} else {
// 			values[i] = field[:j]
// 			i++
// 			field = field[j+1:]
// 		}
// 	}
// 	values = values[:i]
// 	return
// }
