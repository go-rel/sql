package sql

import (
	"context"

	"github.com/go-rel/rel"
)

type IncrementFunc func(Connection) int

// InsertAll adapter.
type InsertAll struct {
	connection       Connection
	insertAllBuilder InsertAllBuilder
	incrementFunc    IncrementFunc
}

// InsertAll inserts multiple records to database and returns its ids.
func (ia InsertAll) InsertAll(ctx context.Context, query rel.Query, primaryField string, fields []string, bulkMutates []map[string]rel.Mutate) ([]interface{}, error) {
	var (
		statement, args = ia.insertAllBuilder.Build(query.Table, primaryField, fields, bulkMutates)
		result, err     = ia.connection.DoExec(ctx, statement, args)
	)

	if err != nil {
		return nil, err
	}

	var (
		id, _ = result.LastInsertId()
		ids   = make([]interface{}, len(bulkMutates))
		inc   = 1
	)

	if ia.incrementFunc != nil {
		inc = ia.incrementFunc(ia.connection)
	}

	if inc < 0 {
		id = id + int64((len(bulkMutates)-1)*inc)
		inc *= -1
	}

	if primaryField != "" {
		counter := 0
		for i := range ids {
			if mut, ok := bulkMutates[i][primaryField]; ok {
				ids[i] = mut.Value
				id = toInt64(ids[i])
				counter = 1
			} else {
				ids[i] = id + int64(counter*inc)
				counter++
			}
		}
	}

	return ids, nil
}

// NewInsertAll adapter.
func NewInsertAll(connection Connection, insertAllBuilder InsertAllBuilder, incrementFunc IncrementFunc) InsertAll {
	return InsertAll{
		connection:       connection,
		insertAllBuilder: insertAllBuilder,
		incrementFunc:    incrementFunc,
	}
}

func toInt64(i interface{}) int64 {
	var result int64

	switch s := i.(type) {
	case int:
		result = int64(s)
	case int64:
		result = s
	case int32:
		result = int64(s)
	case int16:
		result = int64(s)
	case int8:
		result = int64(s)
	case uint:
		result = int64(s)
	case uint64:
		result = int64(s)
	case uint32:
		result = int64(s)
	case uint16:
		result = int64(s)
	case uint8:
		result = int64(s)
	}

	return result
}
