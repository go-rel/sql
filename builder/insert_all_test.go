package builder

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/stretchr/testify/assert"
)

func BenchmarkInsertAll_Build(b *testing.B) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
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
		insertAllBuilder.Build("users", "id", []string{"name"}, bulkMutates, rel.OnConflict{})
	}
}

func TestInsertAll_Build(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
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

	statement, args := insertAllBuilder.Build("users", "id", []string{"name"}, bulkMutates, rel.OnConflict{})
	assert.Equal(t, "INSERT INTO `users` (`name`) VALUES (?),(DEFAULT),(?);", statement)
	assert.Equal(t, []any{"foo", "boo"}, args)

	// with age
	statement, args = insertAllBuilder.Build("users", "id", []string{"name", "age"}, bulkMutates, rel.OnConflict{})
	assert.Equal(t, "INSERT INTO `users` (`name`,`age`) VALUES (?,DEFAULT),(DEFAULT,?),(?,?);", statement)
	assert.Equal(t, []any{"foo", 10, "boo", 20}, args)
}

func TestInsertAll_Build_ordinal(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory:         BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}},
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

	statement, args := insertAllBuilder.Build("users", "id", []string{"name"}, bulkMutates, rel.OnConflict{})
	assert.Equal(t, "INSERT INTO \"users\" (\"name\") VALUES ($1),(DEFAULT),($2) RETURNING \"id\";", statement)
	assert.Equal(t, []any{"foo", "boo"}, args)

	// with age
	statement, args = insertAllBuilder.Build("users", "id", []string{"name", "age"}, bulkMutates, rel.OnConflict{})
	assert.Equal(t, "INSERT INTO \"users\" (\"name\",\"age\") VALUES ($1,DEFAULT),(DEFAULT,$2),($3,$4) RETURNING \"id\";", statement)
	assert.Equal(t, []any{"foo", 10, "boo", 20}, args)
}

func TestInsertAll_Build_onConflictIgnore(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement:       "ON CONFLICT",
				IgnoreStatement: "IGNORE",
				SupportKey:      true,
			},
		}
		bulkMutates = []map[string]rel.Mutate{
			{
				"id": rel.Set("id", 1),
			},
			{
				"id": rel.Set("id", 2),
			},
		}
		onConflict = rel.OnConflict{Keys: []string{"id"}, Ignore: true}
		qs, args   = insertAllBuilder.Build("users", "id", []string{"id"}, bulkMutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?),(?) ON CONFLICT(`id`) IGNORE;", qs)
	assert.Equal(t, []any{1, 2}, args)
}

func TestInsertAll_Build_onConflictReplace(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement:       "ON CONFLICT",
				UpdateStatement: "DO UPDATE SET",
				TableQualifier:  "EXCLUDED",
				SupportKey:      true,
			},
		}
		bulkMutates = []map[string]rel.Mutate{
			{
				"id": rel.Set("id", 1),
			},
			{
				"id": rel.Set("id", 2),
			},
		}
		onConflict = rel.OnConflict{Keys: []string{"id", "username"}, Replace: true}
		qs, args   = insertAllBuilder.Build("users", "id", []string{"id"}, bulkMutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?),(?) ON CONFLICT(`id`,`username`) DO UPDATE SET `id`=`EXCLUDED`.`id`;", qs)
	assert.Equal(t, []any{1, 2}, args)
}

func TestInsertAll_Build_onConflictFragment(t *testing.T) {
	var (
		insertAllBuilder = InsertAll{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement: "ON CONFLICT",
			},
		}
		bulkMutates = []map[string]rel.Mutate{
			{
				"id": rel.Set("id", 1),
			},
			{
				"id": rel.Set("id", 2),
			},
		}
		onConflict = rel.OnConflict{Fragment: "SET `name`=?", FragmentArgs: []any{"foo"}}
		qs, args   = insertAllBuilder.Build("users", "id", []string{"id"}, bulkMutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?),(?) ON CONFLICT SET `name`=?;", qs)
	assert.Equal(t, []any{1, 2, "foo"}, args)
}
