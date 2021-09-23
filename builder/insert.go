package builder

import (
	"github.com/go-rel/rel"
)

// Insert builder.
type Insert struct {
	Name                  Name
	ReturningPrimaryValue bool
	InsertDefaultValues   bool
}

// Build sql query and its arguments.
func (i Insert) Build(table string, primaryField string, mutates map[string]rel.Mutate) (string, []interface{}) {
	var (
		buffer Buffer
		count  = len(mutates)
	)

	buffer.WriteString("INSERT INTO ")
	buffer.WriteString(i.Name.Build(table))

	if count == 0 && i.InsertDefaultValues {
		buffer.WriteString(" DEFAULT VALUES")
	} else {
		// buffer.Arguments = make([]interface{}, count)
		buffer.WriteString(" (")

		n := 0
		for field, mut := range mutates {
			if mut.Type == rel.ChangeSetOp {
				buffer.WriteString(i.Name.Build(field))
			}

			if n < count-1 {
				buffer.WriteByte(',')
			}
			n++
		}

		buffer.WriteString(") VALUES ")

		buffer.WriteByte('(')
		for _, mut := range mutates {
			if mut.Type == rel.ChangeSetOp {
				buffer.WriteValue(mut.Value)
			}

			if n < count-1 {
				buffer.WriteByte(',')
			}
			n++
		}
		buffer.WriteByte(')')
	}

	if i.ReturningPrimaryValue {
		buffer.WriteString(" RETURNING ")
		buffer.WriteString(i.Name.Build(primaryField))
	}

	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}
