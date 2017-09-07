package meter2

import "text/template"

type tplEvent struct {
	*counterEvent
	tpl         *template.Template
	nameLabels  []string
	fieldLabels []string
}

// func NewTemplateEvent(name, text string, labels []string, res Granularity) *tplCounters {
//
// }

func (e *tplEvent) TemplateData(values []string) map[string]string {
	if values == nil {
		return nil
	}
	m := make(map[string]string, len(e.nameLabels))
	labels := e.desc.labels
	n := len(labels)
	if n > len(values) {
		n = len(values)
	}
iloop:
	for i := 0; i < n; i++ {
		label := labels[i]
		for j := 0; j < len(e.nameLabels); j++ {
			if e.nameLabels[j] == label {
				m[label] = values[i]
				continue iloop
			}
		}
	}
	return m
}

func (e *tplEvent) FieldValuesMap(values map[string]string) []string {
	if values == nil {
		return nil
	}
	out := make([]string, len(e.fieldLabels))
	for i := 0; i < len(e.fieldLabels); i++ {
		out[i] = values[e.fieldLabels[i]]
	}
	return out
}

func (e *tplEvent) FieldValues(values ...string) []string {
	labels := e.desc.labels
	n := len(labels)
	if n > len(values) {
		n = len(values)
	}
	k := 0
iloop:
	for i := 0; i < n; i++ {
		label := labels[i]
		for j := 0; j < len(e.nameLabels); j++ {
			if e.nameLabels[j] == label {
				continue iloop
			}
		}
		values[k] = values[i]
		k++
	}
	return values[:k]
}

func (e *tplEvent) RenderName(data map[string]string) (name string, err error) {
	b := bget()
	if err = e.tpl.Execute(b, data); err == nil {
		name = b.String()
	}
	bput(b)
	return
}

func (e *tplEvent) WithLabels(values map[string]string) Metric {
	m, _ := e.FindOrCreate(e.FieldValuesMap(values), DescriptorFunc(func() *Desc {
		if values == nil {
			return e.desc
		}
		d := *e.desc
		d.name, d.err = e.RenderName(values)
		return &d
	}))
	return m
}

func (e *tplEvent) WithLabelValues(values []string) Metric {
	if values == nil {
		return e.counterEvent.WithLabelValues(values)
	}
	m, _ := e.FindOrCreate(e.FieldValues(values...), DescriptorFunc(func() *Desc {
		data := e.TemplateData(values)
		d := *e.desc
		d.name, d.err = e.RenderName(data)
		return &d
	}))
	return m
}

// func NewTplEvent(tpl *template.Template, labels []string, resolutions Granularity) (e Event, err error) {
// 	d := anonymousDesc{name: tpl.Name()}
// 	a := anonymousEvent{d, NewCounters()}
// 	et := tplEvent{anonymousEvent: a}
// 	defer func() {
// 		if err != nil {
// 			d.err = err
// 		}
// 	}()
// 	d.labels = labels
// 	if err = CheckUnique(labels); err != nil {
// 		return
// 	}
// 	if et.tpl, et.nameLabels, err = nameTemplate(tpl, labels); err != nil {
// 		return
// 	}
//
// 	if et.tpl == nil {
// 		b := bget()
// 		if tpl.Execute(b, nil) == nil {
// 			if name := b.String(); name != "" {
// 				d.name = name
// 			}
// 		}
// 		bput(b)
// 	}
//
// 	et.fieldLabels = WithoutLabels(d.labels, et.nameLabels)
//
// 	if d.resolutions, err = CheckEventParams(resolutions, et.fieldLabels); err != nil {
// 		return
// 	}
// 	if err == nil {
// 		e = et
// 	}
// 	return
//
// }
//
// type tplEvent struct {
// 	tpl         *template.Template
// 	fieldLabels []string
// 	nameLabels  []string
// 	anonymousEvent
// }
//
// func (e tplEvent) Describe() Desc {
// 	return e.desc
// }
// func (e tplEvent) Collect(ch chan<- Metric) {
// 	b := bget()
// 	defer bput(b)
// 	labels := e.desc.Labels()
// 	desc := e.desc.(anonymousDesc)
// 	e.counters.FlushEach(func(field string, n int64) {
// 		values := SplitValueFieldN(field, len(labels))
// 		if e.tpl == nil {
// 			ch <- anonymousMetric{values, n, desc}
// 			return
// 		}
// 		d := desc
// 		var data map[string]string
// 		data, values = e.nameValues(values, labels)
// 		b.Reset()
// 		if d.err = e.tpl.Execute(b, data); d.err == nil {
// 			d.name = b.String()
// 		}
// 		ch <- anonymousMetric{values, n, d}
// 	})
// }
//
// func (e tplEvent) nameValues(values, labels []string) (map[string]string, []string) {
// 	if values == nil {
// 		return nil, nil
// 	}
// 	nameValues := make(map[string]string, len(e.nameLabels))
// 	n := len(labels)
// 	if n > len(values) {
// 		n = len(values)
// 	}
// 	j := 0
// iloop:
// 	for i := 0; i < n; i++ {
// 		label := labels[i]
// 		for _, nl := range e.nameLabels {
// 			if nl == label {
// 				nameValues[label] = values[i]
// 				continue iloop
// 			}
// 		}
// 		values[j] = values[i]
// 		j++
//
// 	}
// 	return nameValues, values[:j]
//
// }
//
// // func (e tplEvent) Collect(ch chan<- Metric) {
// // 	e.counters.FlushEach(func(field string, n int64) {
// // 		ch <- e.Metric(field, n)
// // 	})
// // }
//
// // NameTemplate parses a name template and extracts fields from the tree
// func nameTemplate(tpl *template.Template, labels []string) (*template.Template, []string, error) {
// 	labelSet := make(map[string]bool, len(labels))
// 	for _, label := range labels {
// 		labelSet[label] = true
// 	}
// 	tree := tpl.Tree
// 	if len(tree.Root.Nodes) == 1 && tree.Root.Nodes[0].Type() == parse.NodeText {
// 		return nil, nil, nil
// 	}
// 	tplFields := make(map[string]bool)
// 	nls := make([]string, 0, len(labelSet))
// 	treeFields(tree.Root, tplFields)
// 	var err error
// 	for field, _ := range tplFields {
// 		label := field[1:]
// 		if labelSet[label] {
// 			nls = append(nls, label)
// 		} else {
// 			return nil, nil, errors.New("Invalid template field " + field)
// 		}
// 	}
//
// 	name := tpl.Name()
// 	tpl, err = template.New(name).AddParseTree(name, tree)
// 	return tpl, nls, err
// }
// func NameTemplate(name, text string, labels []string) (tpl *template.Template, nls []string, err error) {
// 	if tpl, err = template.New(name).Parse(text); err != nil {
// 		return
// 	}
// 	return nameTemplate(tpl, labels)
// }
//
// func treeFields(n parse.Node, fields map[string]bool) {
// 	switch n.Type() {
// 	case parse.NodeList:
// 		l := n.(*parse.ListNode)
// 		for _, n := range l.Nodes {
// 			treeFields(n, fields)
// 		}
// 	case parse.NodeAction:
// 		treeFields(n.(*parse.ActionNode).Pipe, fields)
// 	case parse.NodePipe:
// 		p := n.(*parse.PipeNode)
// 		for _, n := range p.Cmds {
// 			treeFields(n, fields)
// 		}
// 		for _, n := range p.Decl {
// 			treeFields(n, fields)
// 		}
// 	case parse.NodeCommand:
// 		cmd := n.(*parse.CommandNode)
// 		for _, n := range cmd.Args {
// 			treeFields(n, fields)
// 		}
// 	case parse.NodeField:
// 		field := n.(*parse.FieldNode)
// 		fields[field.String()] = true
// 	}
//
// }
//
// const LeftDelim = "{{"
// const RightDelim = "}}"
//
// // func printTree(n parse.Node, pos int) {
// // 	log.Println(pos, n.Type(), n.String())
// // 	switch n.Type() {
// // 	case parse.NodeList:
// // 		l := n.(*parse.ListNode)
// // 		for i, n := range l.Nodes {
// // 			printTree(n, i)
// // 		}
// // 	case parse.NodeAction:
// // 		printTree(n.(*parse.ActionNode).Pipe, -1)
// // 	case parse.NodePipe:
// // 		p := n.(*parse.PipeNode)
// // 		for i, n := range p.Cmds {
// // 			printTree(n, i)
// // 		}
// // 		for i, n := range p.Decl {
// // 			printTree(n, i)
// // 		}
// // 	case parse.NodeCommand:
// // 		cmd := n.(*parse.CommandNode)
// // 		for i, n := range cmd.Args {
// // 			printTree(n, i)
// // 		}
// // 	}
// //
// // }
