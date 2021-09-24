package builder

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/go-rel/rel/where"
	"github.com/stretchr/testify/assert"
)

func BenchmarkQuery_Build(b *testing.B) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
			Filter:        Filter{},
		}
	)

	for n := 0; n < b.N; n++ {
		query := rel.From("users").
			Select("id", "name").
			Join("transactions").
			Where(where.Eq("id", 10), where.In("status", 1, 2, 3)).
			Group("type").Having(where.Gt("price", 1000)).
			SortAsc("created_at").SortDesc("id").
			Offset(10).Limit(10)

		queryBuilder.Build(query)
	}
}

func TestQuery_Build(t *testing.T) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
			Filter:        Filter{},
		}
		query = rel.From("users")
	)

	tests := []struct {
		QueryString string
		Args        []interface{}
		Query       rel.Query
	}{
		{
			"SELECT * FROM `users`;",
			nil,
			query,
		},
		{
			"SELECT `users`.* FROM `users`;",
			nil,
			query.Select("users.*"),
		},
		{
			"SELECT `id`,`name` FROM `users`;",
			nil,
			query.Select("id", "name"),
		},
		{
			"SELECT `id`,FIELD(`gender`, \"male\") AS `order` FROM `users` ORDER BY `order` ASC;",
			nil,
			query.Select("id", "^FIELD(`gender`, \"male\") AS `order`").SortAsc("order"),
		},
		{
			"SELECT * FROM `users` JOIN `transactions` ON `transactions`.`id`=`users`.`transaction_id`;",
			nil,
			query.JoinOn("transactions", "transactions.id", "users.transaction_id"),
		},
		{
			"SELECT * FROM `users` WHERE `id`=?;",
			[]interface{}{10},
			query.Where(where.Eq("id", 10)),
		},
		{
			"SELECT DISTINCT * FROM `users` GROUP BY `type` HAVING `price`>?;",
			[]interface{}{1000},
			query.Distinct().Group("type").Having(where.Gt("price", 1000)),
		},
		{
			"SELECT * FROM `users` INNER JOIN `transactions` ON `transactions`.`id`=`users`.`transaction_id`;",
			nil,
			query.JoinWith("INNER JOIN", "transactions", "transactions.id", "users.transaction_id"),
		},
		{
			"SELECT * FROM `users` ORDER BY `created_at` ASC;",
			nil,
			query.SortAsc("created_at"),
		},
		{
			"SELECT * FROM `users` ORDER BY `created_at` ASC, `id` DESC;",
			nil,
			query.SortAsc("created_at").SortDesc("id"),
		},
		{
			"SELECT * FROM `users` LIMIT 10 OFFSET 10;",
			nil,
			query.Offset(10).Limit(10),
		},
	}

	for _, test := range tests {
		t.Run(test.QueryString, func(t *testing.T) {
			var (
				qs, args = queryBuilder.Build(test.Query)
			)

			assert.Equal(t, test.QueryString, qs)
			assert.Equal(t, test.Args, args)
		})
	}
}

func TestQuery_Build_ordinal(t *testing.T) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, EscapePrefix: "\"", EscapeSuffix: "\""},
			Filter:        Filter{},
		}
		query = rel.From("users")
	)

	tests := []struct {
		QueryString string
		Args        []interface{}
		Query       rel.Query
	}{
		{
			"SELECT * FROM \"users\";",
			nil,
			query,
		},
		{
			"SELECT \"users\".* FROM \"users\";",
			nil,
			query.Select("users.*"),
		},
		{
			"SELECT \"id\",\"name\" FROM \"users\";",
			nil,
			query.Select("id", "name"),
		},
		{
			"SELECT \"id\" AS \"user_id\",\"name\" FROM \"users\";",
			nil,
			query.Select("id as user_id", "name"),
		},
		{
			"SELECT \"id\" AS \"user_id\",\"name\" FROM \"users\";",
			nil,
			query.Select("id AS user_id", "name"),
		},
		{
			"SELECT * FROM \"users\" JOIN \"transactions\" ON \"transactions\".\"id\"=\"users\".\"transaction_id\";",
			nil,
			query.JoinOn("transactions", "transactions.id", "users.transaction_id"),
		},
		{
			"SELECT * FROM \"users\" WHERE \"id\"=$1;",
			[]interface{}{10},
			query.Where(where.Eq("id", 10)),
		},
		{
			"SELECT DISTINCT * FROM \"users\" GROUP BY \"type\" HAVING \"price\">$1;",
			[]interface{}{1000},
			query.Distinct().Group("type").Having(where.Gt("price", 1000)),
		},
		{
			"SELECT * FROM \"users\" JOIN \"transactions\" ON \"transactions\".\"id\"=\"users\".\"transaction_id\";",
			nil,
			query.JoinOn("transactions", "transactions.id", "users.transaction_id"),
		},
		{
			"SELECT * FROM \"users\" ORDER BY \"created_at\" ASC;",
			nil,
			query.SortAsc("created_at"),
		},
		{
			"SELECT * FROM \"users\" ORDER BY \"created_at\" ASC, \"id\" DESC;",
			nil,
			query.SortAsc("created_at").SortDesc("id"),
		},
		{
			"SELECT * FROM \"users\" LIMIT 10 OFFSET 10;",
			nil,
			query.Offset(10).Limit(10),
		},
	}

	for _, test := range tests {
		t.Run(test.QueryString, func(t *testing.T) {
			var (
				qs, args = queryBuilder.Build(test.Query)
			)

			assert.Equal(t, test.QueryString, qs)
			assert.Equal(t, test.Args, args)
		})
	}
}

func TestQuery_Build_SQLQuery(t *testing.T) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", EscapePrefix: "`", EscapeSuffix: "`"},
			Filter:        Filter{},
		}
		query    = rel.Build("", rel.SQL("SELECT * FROM `users` WHERE id=?;", 1))
		qs, args = queryBuilder.Build(query)
	)

	assert.Equal(t, "SELECT * FROM `users` WHERE id=?;", qs)
	assert.Equal(t, []interface{}{1}, args)
}
