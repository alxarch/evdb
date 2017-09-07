package meter2

// }()
// 	b := bget()
// 	defer bput(b)
// 	tm := time.Now()
// 	ttls := make(map[string]time.Duration)
// 	for p := range ch {
// 		if p == nil || p.Error() != nil {
// 			continue
// 		}
// 		n := p.Count()
// 		if n == 0 {
// 			continue
// 		}
// 		r := p.Resolutions()
// 		if r == nil {
// 			continue
// 		}
// 		labels := p.Labels()
// 		values := p.Values()
// 		name := p.Name()
// 		for res, dims := range r {
// 			b.Reset()
// 			s.AppendKey(res, name, tm, b)
// 			key := b.String()
// 			if values == nil {
// 				pipeline.HIncrBy(key, "*", n)
// 				ttls[key] = res.TTL()
// 				continue
// 			}
// 			if len(values) == 0 || len(dims) == 0 {
// 				continue
// 			}
// 			i := 0
// 			for _, dim := range dims {
// 				b.Reset()
// 				if ok := s.AppendField(dim, labels, values, b); ok {
// 					pipeline.HIncrBy(key, b.String(), n)
// 					i++
// 				}
// 			}
// 			if i > 0 {
// 				ttls[key] = res.TTL()
// 			}
// 		}
// 	}
// 	for key, ttl := range ttls {
// 		if ttl > 0 {
// 			pipeline.PExpire(key, ttl)
// 		}
// 	}
// 	_, err := pipeline.Exec()
// 	result <- err
// }()
// return <-result

// 				pipeline.HIncrBy(key, "*", n)
// 				ttls[key] = res.TTL()
// 				continue
// 			}
// 			if len(values) == 0 || len(dims) == 0 {
// 				continue
// 			}
// 			i := 0
// 			for _, dim := range dims {
// 				b.Reset()
// 				if ok := s.AppendField(dim, labels, values, b); ok {
// 					pipeline.HIncrBy(key, b.String(), n)
// 					i++
// 				}
// 			}
// 			if i > 0 {
// 				ttls[key] = res.TTL()
// 			}
