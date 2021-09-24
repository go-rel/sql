package builder

import (
	"testing"

	"github.com/go-rel/rel/where"
	"github.com/stretchr/testify/assert"
)

func TestDelete_Builder(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"}
		filter        = Filter{}
		deleteBuilder = Delete{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
	)

	qs, args := deleteBuilder.Build("users", where.And())
	assert.Equal(t, "DELETE FROM `users`;", qs)
	assert.Equal(t, []interface{}(nil), args)

	qs, args = deleteBuilder.Build("users", where.Eq("id", 1))
	assert.Equal(t, "DELETE FROM `users` WHERE `id`=?;", qs)
	assert.Equal(t, []interface{}{1}, args)
}

func TestDelete_Builder_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, EscapePrefix: "\"", EscapeSuffix: "\""}
		filter        = Filter{}
		deleteBuilder = Delete{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
	)

	qs, args := deleteBuilder.Build("users", where.And())
	assert.Equal(t, "DELETE FROM \"users\";", qs)
	assert.Equal(t, []interface{}(nil), args)

	qs, args = deleteBuilder.Build("users", where.Eq("id", 1))
	assert.Equal(t, "DELETE FROM \"users\" WHERE \"id\"=$1;", qs)
	assert.Equal(t, []interface{}{1}, args)
}
