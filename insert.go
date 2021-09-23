package sql

import (
	"context"

	"github.com/go-rel/rel"
)

// Insert adapter.
type Insert struct {
	connection    Connection
	insertBuilder InsertBuilder
}

// Insert inserts a record to database and returns its id.
func (i Insert) Insert(ctx context.Context, query rel.Query, primaryField string, mutates map[string]rel.Mutate) (interface{}, error) {
	var (
		statement, args = i.insertBuilder.Build(query.Table, primaryField, mutates)
		id, err         = i.connection.DoExec(ctx, statement, args)
	)

	return id, err
}

// NewInsert adapter.
func NewInsert(connection Connection, insertBuilder InsertBuilder) Insert {
	return Insert{
		connection:    connection,
		insertBuilder: insertBuilder,
	}
}
