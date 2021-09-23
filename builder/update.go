package builder

import (
	"github.com/go-rel/rel"
)

// Update builder.
type Update struct {
	Name   Name
	Query  Query
	Filter Filter
}

// Build SQL string and it arguments.
func (u Update) Build(table string, primaryField string, mutates map[string]rel.Mutate, filter rel.FilterQuery) (string, []interface{}) {
	var (
		buffer Buffer
	)

	buffer.WriteString("UPDATE ")
	buffer.WriteString(u.Name.Build(table))
	buffer.WriteString(" SET ")

	i := 0
	for field, mut := range mutates {
		if field == primaryField {
			continue
		}

		if i > 0 {
			buffer.WriteByte(',')
		}
		i++

		switch mut.Type {
		case rel.ChangeSetOp:
			buffer.WriteString(u.Name.Build(field))
			buffer.WriteByte('=')
			buffer.WriteValue(mut.Value)
		case rel.ChangeIncOp:
			buffer.WriteString(u.Name.Build(field))
			buffer.WriteByte('=')
			buffer.WriteString(u.Name.Build(field))
			buffer.WriteByte('+')
			buffer.WriteValue(mut.Value)
		case rel.ChangeFragmentOp:
			buffer.WriteString(field)
			buffer.AddArguments(mut.Value.([]interface{})...)
		}
	}

	if !filter.None() {
		buffer.WriteString(" WHERE ")
		u.Filter.Write(&buffer, filter, u.Query)
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}
