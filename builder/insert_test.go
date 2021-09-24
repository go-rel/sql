package builder

import (
	"fmt"
	"testing"

	"github.com/go-rel/rel"
	"github.com/stretchr/testify/assert"
)

func BenchmarkInsert_Build(b *testing.B) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
	)

	for n := 0; n < b.N; n++ {
		insertBuilder.Build("users", "id", mutates)
	}
}

func TestInsert_Build(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
		qs, args = insertBuilder.Build("users", "id", mutates)
	)

	assert.Regexp(t, fmt.Sprint(`^INSERT INTO `, "`users`", ` \((`, "`", `\w*`, "`", `,?){3}\) VALUES \(\?,\?,\?\);`), qs)
	assert.Contains(t, qs, "name")
	assert.Contains(t, qs, "age")
	assert.Contains(t, qs, "agree")
	assert.ElementsMatch(t, []interface{}{"foo", 10, true}, args)
}

func TestInsert_Build_ordinal(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory:         BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, EscapePrefix: "\"", EscapeSuffix: "\""},
			InsertDefaultValues:   true,
			ReturningPrimaryValue: true,
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
		qs, args = insertBuilder.Build("users", "id", mutates)
	)

	assert.Regexp(t, `^INSERT INTO \"users\" \(("\w*",?){3}\) VALUES \(\$1,\$2,\$3\) RETURNING \"id\";`, qs)
	assert.Contains(t, qs, "name")
	assert.Contains(t, qs, "age")
	assert.Contains(t, qs, "agree")
	assert.ElementsMatch(t, []interface{}{"foo", 10, true}, args)
}

func TestInsert_Build_defaultValuesDisabled(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
		}
		mutates  = map[string]rel.Mutate{}
		qs, args = insertBuilder.Build("users", "id", mutates)
	)

	assert.Equal(t, "INSERT INTO `users` () VALUES ();", qs)
	assert.Equal(t, []interface{}{}, args)
}

func TestInsert_Build_defaultValuesEnabled(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory:         BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
			ReturningPrimaryValue: true,
			InsertDefaultValues:   true,
		}
		mutates  = map[string]rel.Mutate{}
		qs, args = insertBuilder.Build("users", "id", mutates)
	)

	assert.Equal(t, "INSERT INTO `users` DEFAULT VALUES RETURNING `id`;", qs)
	assert.Nil(t, args)
}
