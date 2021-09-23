package sql

import (
	"context"

	"github.com/go-rel/rel"
	adapter "github.com/go-rel/rel/adapter/sql"
)

// Query adapter.
type Query struct {
	connection   Connection
	queryBuilder QueryBuilder
	errorMapper  ErrorMapper
}

// Query performs query operation.
func (q Query) Query(ctx context.Context, query rel.Query) (rel.Cursor, error) {
	var (
		statement, args = q.queryBuilder.Build(query)
		rows, err       = q.connection.DoQuery(ctx, statement, args)
	)

	return &adapter.Cursor{Rows: rows}, q.errorMapper(err)
}

// NewQueryAdapter adapter.
func NewQueryAdapter(connection Connection, queryBuilder QueryBuilder, errorMapper ErrorMapper) Query {
	return Query{
		connection:   connection,
		queryBuilder: queryBuilder,
		errorMapper:  errorMapper,
	}
}
