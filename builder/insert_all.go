package builder

import (
	"github.com/go-rel/rel"
)

// InsertAll builder.
type InsertAll struct {
	Name                  Name
	ReturningPrimaryValue bool
}

// Build SQL string and its arguments.
func (ia InsertAll) Build(table string, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer       Buffer
		fieldsCount  = len(fields)
		mutatesCount = len(bulkMutates)
	)

	// buffer.Arguments = make([]interface{}, 0, fieldsCount*mutatesCount)

	buffer.WriteString("INSERT INTO ")

	buffer.WriteString(ia.Name.Build(table))
	buffer.WriteString(" (")

	for i := range fields {
		buffer.WriteString(ia.Name.Build(fields[i]))

		if i < fieldsCount-1 {
			buffer.WriteByte(',')
		}
	}

	buffer.WriteString(") VALUES ")

	for i, mutates := range bulkMutates {
		buffer.WriteByte('(')

		for j, field := range fields {
			if mut, ok := mutates[field]; ok && mut.Type == rel.ChangeSetOp {
				buffer.WriteValue(mut.Value)
			} else {
				buffer.WriteString("DEFAULT")
			}

			if j < fieldsCount-1 {
				buffer.WriteByte(',')
			}
		}

		if i < mutatesCount-1 {
			buffer.WriteString("),")
		} else {
			buffer.WriteByte(')')
		}
	}

	if ia.ReturningPrimaryValue {
		buffer.WriteString(" RETURNING ")
		buffer.WriteString(ia.Name.Build(primaryField))
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()

}
