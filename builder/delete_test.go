package builder

import (
	"testing"

	"github.com/go-rel/rel/where"
	"github.com/stretchr/testify/assert"
)

func TestDelete_Builder(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		filter        = Filter{}
		deleteBuilder = Delete{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
	)

	qs, args := deleteBuilder.Build("users", where.And())
	assert.Equal(t, "DELETE FROM `users`;", qs)
	assert.Equal(t, []any(nil), args)

	qs, args = deleteBuilder.Build("users", where.Eq("id", 1))
	assert.Equal(t, "DELETE FROM `users` WHERE `users`.`id`=?;", qs)
	assert.Equal(t, []any{1}, args)
}

func TestDelete_Builder_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}}
		filter        = Filter{}
		deleteBuilder = Delete{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
	)

	qs, args := deleteBuilder.Build("users", where.And())
	assert.Equal(t, "DELETE FROM \"users\";", qs)
	assert.Equal(t, []any(nil), args)

	qs, args = deleteBuilder.Build("users", where.Eq("id", 1))
	assert.Equal(t, "DELETE FROM \"users\" WHERE \"users\".\"id\"=$1;", qs)
	assert.Equal(t, []any{1}, args)
}
