package meter2

// func JSONWriteObject(b *bytes.Buffer, pairs []string) {
// 	n := len(pairs)
// 	n -= n % 2
// 	for i := 0; i < n; i += 2 {
// 		if i == 0 {
// 			b.WriteByte('{')
// 		} else {
// 			b.WriteByte(',')
// 		}
// 		b.WriteByte('"')
// 		b.WriteString(pairs[i])
// 		b.WriteByte('"')
// 		b.WriteByte(':')
// 		b.WriteByte('"')
// 		b.WriteString(pairs[i+1])
// 		b.WriteByte('"')
// 	}
// 	if n != 0 {
// 		b.WriteByte('}')
// 	}
// }
