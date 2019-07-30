package evutil

import db "github.com/alxarch/evdb"

// ZipFields creates a field collection zipping labels and values
func ZipFields(labels []string, values []string) (fields db.Fields) {
	for i, label := range labels {
		if 0 <= i && i < len(values) {
			fields = append(fields, db.Field{
				Label: label,
				Value: values[i],
			})
		} else {
			fields = append(fields, db.Field{
				Label: label,
			})
		}
	}
	return

}
