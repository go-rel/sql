package sql

import (
	"context"

	"github.com/go-rel/rel"
)

// Delete adapter.
type Delete struct {
	exec          Exec
	deleteBuilder DeleteBuilder
}

// Delete deletes all results that match the query.
func (d Delete) Delete(ctx context.Context, query rel.Query) (int, error) {
	var (
		statement, args      = d.deleteBuilder.Build(query.Table, query.WhereQuery)
		_, deletedCount, err = d.exec.Exec(ctx, statement, args)
	)

	return int(deletedCount), err
}

// NewDelete adapter.
func NewDelete(exec Exec, deleteBuilder DeleteBuilder) Delete {
	return Delete{
		exec:          exec,
		deleteBuilder: deleteBuilder,
	}
}
