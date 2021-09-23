package sql

import (
	"context"
	"database/sql"
	"errors"
	"strconv"

	"github.com/go-rel/rel"
)

// AdapterFactory function.
type AdapterFactory func(db *sql.DB, tx *sql.Tx, savepoint int) rel.Adapter

// Connection adapter.
type Connection struct {
	InstrumentationAdapter *InstrumentationAdapter
	DB                     *sql.DB
	Tx                     *sql.Tx
	Savepoint              int
	AdapterFactory         AdapterFactory
}

// DoExec using active database connection.
func (c Connection) DoExec(ctx context.Context, statement string, args []interface{}) (sql.Result, error) {
	var (
		err    error
		result sql.Result
		finish = c.InstrumentationAdapter.Instrumenter.Observe(ctx, "adapter-exec", statement)
	)

	if c.Tx != nil {
		result, err = c.Tx.ExecContext(ctx, statement, args...)
	} else {
		result, err = c.DB.ExecContext(ctx, statement, args...)
	}

	finish(err)
	return result, err
}

// DoQuery using active database connection.
func (c Connection) DoQuery(ctx context.Context, statement string, args []interface{}) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	finish := c.InstrumentationAdapter.Instrumenter.Observe(ctx, "adapter-query", statement)
	if c.Tx != nil {
		rows, err = c.Tx.QueryContext(ctx, statement, args...)
	} else {
		rows, err = c.DB.QueryContext(ctx, statement, args...)
	}
	finish(err)

	return rows, err
}

// Begin begins a new transaction.
func (c Connection) Begin(ctx context.Context) (rel.Adapter, error) {
	var (
		Tx        *sql.Tx
		savepoint int
		err       error
	)

	finish := c.InstrumentationAdapter.Instrumenter.Observe(ctx, "adapter-begin", "begin transaction")

	if c.Tx != nil {
		Tx = c.Tx
		savepoint = c.Savepoint + 1
		_, err = c.Tx.ExecContext(ctx, "SAVEPOINT s"+strconv.Itoa(savepoint)+";")
	} else {
		Tx, err = c.DB.BeginTx(ctx, nil)
	}

	finish(err)

	newAdapter := c.AdapterFactory(nil, Tx, savepoint)
	newAdapter.Instrumentation(c.InstrumentationAdapter.Instrumenter)

	return newAdapter, err
}

// Commit commits current transaction.
func (c Connection) Commit(ctx context.Context) error {
	var err error

	finish := c.InstrumentationAdapter.Instrumenter.Observe(ctx, "adapter-commit", "commit transaction")

	if c.Tx == nil {
		err = errors.New("unable to commit outside transaction")
	} else if c.Savepoint > 0 {
		_, err = c.Tx.ExecContext(ctx, "RELEASE SAVEPOINT s"+strconv.Itoa(c.Savepoint)+";", []interface{}{})
	} else {
		err = c.Tx.Commit()
	}

	finish(err)

	return err
}

// Rollback revert current transaction.
func (c Connection) Rollback(ctx context.Context) error {
	var err error

	finish := c.InstrumentationAdapter.Instrumenter.Observe(ctx, "adapter-rollback", "rollback transaction")

	if c.Tx == nil {
		err = errors.New("unable to rollback outside transaction")
	} else if c.Savepoint > 0 {
		_, err = c.Tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT s"+strconv.Itoa(c.Savepoint)+";")
	} else {
		err = c.Tx.Rollback()
	}

	finish(err)

	return err
}

// Ping database.
func (c Connection) Ping(ctx context.Context) error {
	return c.DB.PingContext(ctx)
}

// Close database connection.
func (c Connection) Close() error {
	return c.DB.Close()
}
