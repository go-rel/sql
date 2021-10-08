package builder

import (
	"fmt"
	"testing"

	"github.com/go-rel/rel"
	"github.com/go-rel/rel/where"
	"github.com/stretchr/testify/assert"
)

func TestUpdate_Build(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: &Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		filter        = Filter{}
		updateBuilder = Update{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
		mutates = map[string]rel.Mutate{
			"id":    rel.Set("id", 10),
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
	)

	qs, qargs := updateBuilder.Build("users", "id", mutates, where.And())
	assert.Regexp(t, fmt.Sprint("UPDATE `users` SET `", `\w*`, "`=", `\?`, ",`", `\w*`, "`=", `\?`, ",`", `\w*`, "`=", `\?`, ";"), qs)
	assert.ElementsMatch(t, []interface{}{"foo", 10, true}, qargs)

	qs, qargs = updateBuilder.Build("users", "id", mutates, where.Eq("id", 1))
	assert.Regexp(t, fmt.Sprint("UPDATE `users` SET `", `\w*`, "`=", `\?`, ",`", `\w*`, "`=", `\?`, ",`", `\w*`, "`=", `\?`, " WHERE `id`=", `\?`, ";"), qs)
	assert.ElementsMatch(t, []interface{}{"foo", 10, true, 1}, qargs)
}

func TestUpdate_Build_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: &Quote{IDPrefix: "\"", IDSuffix: "\""}}
		filter        = Filter{}
		updateBuilder = Update{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
	)

	qs, args := updateBuilder.Build("users", "id", mutates, where.And())
	assert.Regexp(t, `UPDATE "users" SET "\w*"=\$1,"\w*"=\$2,"\w*"=\$3;`, qs)
	assert.ElementsMatch(t, []interface{}{"foo", 10, true}, args)

	qs, args = updateBuilder.Build("users", "id", mutates, where.Eq("id", 1))
	assert.Regexp(t, `UPDATE "users" SET "\w*"=\$1,"\w*"=\$2,"\w*"=\$3 WHERE "id"=\$4;`, qs)
	assert.ElementsMatch(t, []interface{}{"foo", 10, true, 1}, args)
}

func TestUpdate_Build_incDecAndFragment(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: &Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		filter        = Filter{}
		updateBuilder = Update{
			BufferFactory: bufferFactory,
			Query:         Query{BufferFactory: bufferFactory, Filter: filter},
			Filter:        filter,
		}
	)

	qs, qargs := updateBuilder.Build("users", "id", map[string]rel.Mutate{"age": rel.Inc("age")}, where.And())
	assert.Equal(t, "UPDATE `users` SET `age`=`age`+?;", qs)
	assert.Equal(t, []interface{}{1}, qargs)

	qs, qargs = updateBuilder.Build("users", "id", map[string]rel.Mutate{"age=?": rel.SetFragment("age=?", 10)}, where.And())
	assert.Equal(t, "UPDATE `users` SET age=?;", qs)
	assert.Equal(t, []interface{}{10}, qargs)
}
