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
		buffer.WriteString(" (")

		var (
			n         = 0
			arguments = make([]interface{}, 0, count)
		)

		for field, mut := range mutates {
			if mut.Type != rel.ChangeSetOp {
				continue
			}

			if n > 0 {
				buffer.WriteByte(',')
			}

			buffer.WriteString(i.Name.Build(field))
			arguments = append(arguments, mut.Value)
			n++
		}

		buffer.WriteString(") VALUES (")

		for i, arg := range arguments {
			if i > 0 {
				buffer.WriteByte(',')
			}

			buffer.WriteValue(arg)
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
