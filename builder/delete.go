package builder

import (
	"github.com/go-rel/rel"
)

// Delete builder.
type Delete struct {
	Name   Name
	Query  Query
	Filter Filter
}

// Build SQL query and its arguments.
func (ds Delete) Build(table string, filter rel.FilterQuery) (string, []interface{}) {
	var buffer Buffer

	buffer.WriteString("DELETE FROM ")
	buffer.WriteString(ds.Name.Build(table))

	if !filter.None() {
		buffer.WriteString(" WHERE ")
		ds.Filter.Write(&buffer, filter, ds.Query)
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}
