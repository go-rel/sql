package sql

import (
	"context"
	"database/sql"

	"github.com/go-rel/rel"
)

// Aggregate adapter.
type Aggregate struct {
	connection   Connection
	queryBuilder QueryBuilder
}

// Aggregate record using given query.
func (a Aggregate) Aggregate(ctx context.Context, query rel.Query, mode string, field string) (int, error) {
	var (
		out             sql.NullInt64
		aggregateField  = "^" + mode + "(" + field + ") AS result"
		aggregateQuery  = query.Select(append([]string{aggregateField}, query.GroupQuery.Fields...)...)
		statement, args = a.queryBuilder.Build(aggregateQuery)
		rows, err       = a.connection.DoQuery(ctx, statement, args)
	)

	defer rows.Close()
	if err == nil && rows.Next() {
		rows.Scan(&out)
	}

	return int(out.Int64), err
}

// NewAggregate adapter.
func NewAggregate(connection Connection, queryBuilder QueryBuilder) Aggregate {
	return Aggregate{
		connection:   connection,
		queryBuilder: queryBuilder,
	}
}
