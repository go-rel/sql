package sql

import (
	"context"
)

// ErrorMapper function.
type ErrorMapper func(error) error

// Exec adapter.
type Exec struct {
	connection  Connection
	errorMapper ErrorMapper
}

// Exec performs exec operation.
func (e Exec) Exec(ctx context.Context, statement string, args []interface{}) (int64, int64, error) {
	var (
		res, err = e.connection.DoExec(ctx, statement, args)
	)

	if err != nil {
		return 0, 0, e.errorMapper(err)
	}

	lastID, _ := res.LastInsertId()
	rowCount, _ := res.RowsAffected()

	return lastID, rowCount, nil
}

// NewExec adapter.
func NewExec(connection Connection, errorMapper ErrorMapper) Exec {
	return Exec{
		connection:  connection,
		errorMapper: errorMapper,
	}
}
