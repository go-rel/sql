package sql

import (
	"context"

	"github.com/go-rel/rel"
)

// Update component.
type Update struct {
	exec          Exec
	updateBuilder UpdateBuilder
}

// Update updates a record in database.
func (u Update) Update(ctx context.Context, query rel.Query, primaryField string, mutates map[string]rel.Mutate) (int, error) {
	var (
		statement, args      = u.updateBuilder.Build(query.Table, primaryField, mutates, query.WhereQuery)
		_, updatedCount, err = u.exec.Exec(ctx, statement, args)
	)

	return int(updatedCount), err
}

// NewUpdateAdapter component.
func NewUpdateAdapter(exec Exec, updateBuilder UpdateBuilder) Update {
	return Update{
		exec:          exec,
		updateBuilder: updateBuilder,
	}
}
