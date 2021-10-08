package builder

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/stretchr/testify/assert"
)

func BenchmarkInsertAll_Build(b *testing.B) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: &Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
		}
		bulkMutates = []map[string]rel.Mutate{
			{
				"name": rel.Set("name", "foo"),
			},
			{
				"age": rel.Set("age", 10),
			},
			{
				"name": rel.Set("name", "boo"),
				"age":  rel.Set("age", 20),
			},
		}
	)

	for n := 0; n < b.N; n++ {
		insertAllBuilder.Build("users", "id", []string{"name"}, bulkMutates)
	}
}

func TestInsertAll_Build(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: &Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
		}
		bulkMutates = []map[string]rel.Mutate{
			{
				"name": rel.Set("name", "foo"),
			},
			{
				"age": rel.Set("age", 10),
			},
			{
				"name": rel.Set("name", "boo"),
				"age":  rel.Set("age", 20),
			},
		}
	)

	statement, args := insertAllBuilder.Build("users", "id", []string{"name"}, bulkMutates)
	assert.Equal(t, "INSERT INTO `users` (`name`) VALUES (?),(DEFAULT),(?);", statement)
	assert.Equal(t, []interface{}{"foo", "boo"}, args)

	// with age
	statement, args = insertAllBuilder.Build("users", "id", []string{"name", "age"}, bulkMutates)
	assert.Equal(t, "INSERT INTO `users` (`name`,`age`) VALUES (?,DEFAULT),(DEFAULT,?),(?,?);", statement)
	assert.Equal(t, []interface{}{"foo", 10, "boo", 20}, args)
}

func TestInsertAll_Build_ordinal(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory:         BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: &Quote{IDPrefix: "\"", IDSuffix: "\""}},
			ReturningPrimaryValue: true,
		}
		bulkMutates = []map[string]rel.Mutate{
			{
				"name": rel.Set("name", "foo"),
			},
			{
				"age": rel.Set("age", 10),
			},
			{
				"name": rel.Set("name", "boo"),
				"age":  rel.Set("age", 20),
			},
		}
	)

	statement, args := insertAllBuilder.Build("users", "id", []string{"name"}, bulkMutates)
	assert.Equal(t, "INSERT INTO \"users\" (\"name\") VALUES ($1),(DEFAULT),($2) RETURNING \"id\";", statement)
	assert.Equal(t, []interface{}{"foo", "boo"}, args)

	// with age
	statement, args = insertAllBuilder.Build("users", "id", []string{"name", "age"}, bulkMutates)
	assert.Equal(t, "INSERT INTO \"users\" (\"name\",\"age\") VALUES ($1,DEFAULT),(DEFAULT,$2),($3,$4) RETURNING \"id\";", statement)
	assert.Equal(t, []interface{}{"foo", 10, "boo", 20}, args)
}
