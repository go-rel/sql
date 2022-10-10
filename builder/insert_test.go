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
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
	)

	for n := 0; n < b.N; n++ {
		insertBuilder.Build("users", "id", mutates, rel.OnConflict{})
	}
}

func TestInsert_Build(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
		qs, args = insertBuilder.Build("users", "id", mutates, rel.OnConflict{})
	)

	assert.Regexp(t, fmt.Sprint(`^INSERT INTO `, "`users`", ` \((`, "`", `\w*`, "`", `,?){3}\) VALUES \(\?,\?,\?\);`), qs)
	assert.Contains(t, qs, "name")
	assert.Contains(t, qs, "age")
	assert.Contains(t, qs, "agree")
	assert.ElementsMatch(t, []any{"foo", 10, true}, args)
}

func TestInsert_Build_ordinal(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory:         BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}},
			InsertDefaultValues:   true,
			ReturningPrimaryValue: true,
		}
		mutates = map[string]rel.Mutate{
			"name":  rel.Set("name", "foo"),
			"age":   rel.Set("age", 10),
			"agree": rel.Set("agree", true),
		}
		qs, args = insertBuilder.Build("users", "id", mutates, rel.OnConflict{})
	)

	assert.Regexp(t, `^INSERT INTO \"users\" \(("\w*",?){3}\) VALUES \(\$1,\$2,\$3\) RETURNING \"id\";`, qs)
	assert.Contains(t, qs, "name")
	assert.Contains(t, qs, "age")
	assert.Contains(t, qs, "agree")
	assert.ElementsMatch(t, []any{"foo", 10, true}, args)
}

func TestInsert_Build_defaultValuesDisabled(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
		}
		mutates  = map[string]rel.Mutate{}
		qs, args = insertBuilder.Build("users", "id", mutates, rel.OnConflict{})
	)

	assert.Equal(t, "INSERT INTO `users` () VALUES ();", qs)
	assert.Equal(t, []any{}, args)
}

func TestInsert_Build_defaultValuesEnabled(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory:         BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			ReturningPrimaryValue: true,
			InsertDefaultValues:   true,
		}
		mutates  = map[string]rel.Mutate{}
		qs, args = insertBuilder.Build("users", "id", mutates, rel.OnConflict{})
	)

	assert.Equal(t, "INSERT INTO `users` DEFAULT VALUES RETURNING `id`;", qs)
	assert.Nil(t, args)
}

func TestInsert_Build_onConflictIgnore(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement:       "ON CONFLICT",
				IgnoreStatement: "IGNORE",
				SupportKey:      true,
			},
		}
		mutates = map[string]rel.Mutate{
			"id": rel.Set("id", 1),
		}
		onConflict = rel.OnConflict{Keys: []string{"id"}, Ignore: true}
		qs, args   = insertBuilder.Build("users", "id", mutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?) ON CONFLICT(`id`) IGNORE;", qs)
	assert.Equal(t, []any{1}, args)
}

func TestInsert_Build_onConflictIgnoreSelfAssign(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement:       "ON DUPLICATE KEY",
				UpdateStatement: "UPDATE",
			},
		}
		mutates = map[string]rel.Mutate{
			"id": rel.Set("id", 1),
		}
		onConflict = rel.OnConflict{Keys: []string{"id"}, Ignore: true}
		qs, args   = insertBuilder.Build("users", "id", mutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?) ON DUPLICATE KEY UPDATE `id`=`id`;", qs)
	assert.Equal(t, []any{1}, args)
}

func TestInsert_Build_onConflictReplace(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement:       "ON CONFLICT",
				UpdateStatement: "DO UPDATE SET",
				TableQualifier:  "EXCLUDED",
				SupportKey:      true,
			},
		}
		mutates = map[string]rel.Mutate{
			"id": rel.Set("id", 1),
		}
		onConflict = rel.OnConflict{Keys: []string{"id", "username"}, Replace: true}
		qs, args   = insertBuilder.Build("users", "id", mutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?) ON CONFLICT(`id`,`username`) DO UPDATE SET `id`=`EXCLUDED`.`id`;", qs)
	assert.Equal(t, []any{1}, args)
}

func TestInsert_Build_onConflictReplaceUseValues(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement:       "ON DUPLICATE KEY",
				UpdateStatement: "UPDATE",
				UseValues:       true,
			},
		}
		mutates = map[string]rel.Mutate{
			"id":   rel.Set("id", 1),
			"name": rel.Set("id", "foo"),
		}
		onConflict = rel.OnConflict{Keys: []string{"id"}, Replace: true}
		qs, args   = insertBuilder.Build("users", "id", mutates, onConflict)
	)

	assert.Contains(t, []string{
		"INSERT INTO `users` (`id`,`name`) VALUES (?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`);",
		"INSERT INTO `users` (`name`,`id`) VALUES (?,?) ON DUPLICATE KEY UPDATE `id`=VALUES(`id`),`name`=VALUES(`name`);",
		"INSERT INTO `users` (`id`,`name`) VALUES (?,?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`),`id`=VALUES(`id`);",
		"INSERT INTO `users` (`name`,`id`) VALUES (?,?) ON DUPLICATE KEY UPDATE `name`=VALUES(`name`),`id`=VALUES(`id`);",
	}, qs)
	assert.Contains(t, []any{
		[]any{1, "foo"},
		[]any{"foo", 1},
	}, args)
}

func TestInsert_Build_onConflictFragment(t *testing.T) {
	var (
		insertBuilder = Insert{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			OnConflict: OnConflict{
				Statement: "ON CONFLICT",
			},
		}
		mutates = map[string]rel.Mutate{
			"id": rel.Set("id", 1),
		}
		onConflict = rel.OnConflict{Fragment: "SET `name`=?", FragmentArgs: []any{"foo"}}
		qs, args   = insertBuilder.Build("users", "id", mutates, onConflict)
	)

	assert.Equal(t, "INSERT INTO `users` (`id`) VALUES (?) ON CONFLICT SET `name`=?;", qs)
	assert.Equal(t, []any{1, "foo"}, args)
}
