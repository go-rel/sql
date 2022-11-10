package builder

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/go-rel/rel/sort"
	"github.com/go-rel/rel/where"
	"github.com/stretchr/testify/assert"
)

func BenchmarkQuery_Build(b *testing.B) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
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
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			Filter:        Filter{},
		}
		query = rel.From("users")
	)

	tests := []struct {
		result string
		args   []any
		query  rel.Query
	}{
		{
			result: "SELECT `users`.* FROM `users`;",
			query:  query,
		},
		{
			result: "SELECT `users`.* FROM `users`;",
			query:  query.Select("users.*"),
		},
		{
			result: "SELECT `users`.`id`,`users`.`name` FROM `users`;",
			query:  query.Select("id", "name"),
		},
		{
			result: "SELECT `users`.`id`,FIELD(`gender`, \"male\") AS `order` FROM `users` ORDER BY order ASC;",
			query:  query.Select("id", "^FIELD(`gender`, \"male\") AS `order`").SortAsc("^order"),
		},
		{
			result: "SELECT `users`.`id`,FIELD(`gender`, \"male\") AS `order` FROM `users` ORDER BY 2 ASC;",
			query:  query.Select("id", "^FIELD(`gender`, \"male\") AS `order`").SortAsc("2"),
		},
		{
			result: "SELECT `users`.* FROM `users` JOIN `transactions` ON `transactions`.`id`=`users`.`transaction_id`;",
			query:  query.JoinOn("transactions", "transactions.id", "users.transaction_id"),
		},
		{
			result: "SELECT `users`.* FROM `users` WHERE `users`.`id`=?;",
			args:   []any{10},
			query:  query.Where(where.Eq("id", 10)),
		},
		{
			result: "SELECT DISTINCT `users`.* FROM `users` GROUP BY `users`.`type` HAVING `users`.`price`>?;",
			args:   []any{1000},
			query:  query.Distinct().Group("type").Having(where.Gt("price", 1000)),
		},
		{
			result: "SELECT `users`.* FROM `users` INNER JOIN `transactions` ON `transactions`.`id`=`users`.`transaction_id`;",
			query:  query.JoinWith("INNER JOIN", "transactions", "transactions.id", "users.transaction_id"),
		},
		{
			result: "SELECT `users`.* FROM `users` INNER JOIN `transactions` ON `transactions`.`id`=`users`.`transaction_id` AND (`transactions`.`status`=? AND `users`.`type`=?) WHERE `users`.`id`=?;",
			args:   []any{1, 2, 10},
			query:  query.JoinWith("INNER JOIN", "transactions", "transactions.id", "users.transaction_id", rel.Eq("status", 1), rel.Eq("users.type", 2)).Where(rel.Eq("id", 10)),
		},
		{
			result: "SELECT `users`.* FROM `users` ORDER BY `users`.`created_at` ASC;",
			query:  query.SortAsc("created_at"),
		},
		{
			result: "SELECT `users`.* FROM `users` ORDER BY `users`.`created_at` ASC, `users`.`id` DESC;",
			query:  query.SortAsc("created_at").SortDesc("id"),
		},
		{
			result: "SELECT `users`.* FROM `users` LIMIT 10 OFFSET 10;",
			query:  query.Offset(10).Limit(10),
		},
		{
			result: "SELECT `users`.* FROM `users` FOR UPDATE;",
			query:  rel.From("users").Lock("FOR UPDATE"),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				result, args = queryBuilder.Build(test.query)
			)

			assert.Equal(t, test.result, result)
			assert.Equal(t, test.args, args)
		})
	}
}

func TestQuery_Build_ordinal(t *testing.T) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}},
			Filter:        Filter{},
		}
		query = rel.From("users")
	)

	tests := []struct {
		result string
		args   []any
		query  rel.Query
	}{
		{
			result: "SELECT \"users\".* FROM \"users\";",
			query:  query,
		},
		{
			result: "SELECT \"users\".* FROM \"users\";",
			query:  query.Select("users.*"),
		},
		{
			result: "SELECT \"users\".\"id\",\"users\".\"name\" FROM \"users\";",
			query:  query.Select("id", "name"),
		},
		{
			result: "SELECT \"users\".\"id\" AS \"user_id\",\"users\".\"name\" FROM \"users\";",
			query:  query.Select("id as user_id", "name"),
		},
		{
			result: "SELECT \"users\".\"id\" AS \"user_id\",\"users\".\"name\" FROM \"users\";",
			query:  query.Select("id AS user_id", "name"),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" JOIN \"transactions\" ON \"transactions\".\"id\"=\"users\".\"transaction_id\";",
			query:  query.JoinOn("transactions", "transactions.id", "users.transaction_id"),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" WHERE \"users\".\"id\"=$1;",
			args:   []any{10},
			query:  query.Where(where.Eq("id", 10)),
		},
		{
			result: "SELECT DISTINCT \"users\".* FROM \"users\" GROUP BY \"users\".\"type\" HAVING \"users\".\"price\">$1;",
			args:   []any{1000},
			query:  query.Distinct().Group("type").Having(where.Gt("price", 1000)),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" JOIN \"transactions\" ON \"transactions\".\"id\"=\"users\".\"transaction_id\";",
			query:  query.JoinOn("transactions", "transactions.id", "users.transaction_id"),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" ORDER BY \"users\".\"created_at\" ASC;",
			query:  query.SortAsc("created_at"),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" ORDER BY \"users\".\"created_at\" ASC, \"users\".\"id\" DESC;",
			query:  query.SortAsc("created_at").SortDesc("id"),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" LIMIT 10 OFFSET 10;",
			query:  query.Offset(10).Limit(10),
		},
		{
			result: "SELECT \"users\".* FROM \"users\" FOR UPDATE;",
			query:  rel.From("users").Lock("FOR UPDATE"),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				qs, args = queryBuilder.Build(test.query)
			)

			assert.Equal(t, test.result, qs)
			assert.Equal(t, test.args, args)
		})
	}
}

func TestQuery_Build_SQLQuery(t *testing.T) {
	var (
		queryBuilder = Query{
			BufferFactory: BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}},
			Filter:        Filter{},
		}
		query    = rel.Build("", rel.SQL("SELECT * FROM `users` WHERE id=?;", 1))
		qs, args = queryBuilder.Build(query)
	)

	assert.Equal(t, "SELECT * FROM `users` WHERE id=?;", qs)
	assert.Equal(t, []any{1}, args)
}

func TestQuery_WriteSelect(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		table       string
		result      string
		selectQuery rel.SelectQuery
	}{
		{
			result: "SELECT *",
		},
		{
			result:      "SELECT *",
			selectQuery: rel.SelectQuery{Fields: []string{"*"}},
		},
		{
			result:      "SELECT `id`,`name`",
			selectQuery: rel.SelectQuery{Fields: []string{"id", "name"}},
		},
		{
			result:      "SELECT DISTINCT *",
			selectQuery: rel.SelectQuery{Fields: []string{"*"}, OnlyDistinct: true},
		},
		{
			result:      "SELECT DISTINCT `id`,`name`",
			selectQuery: rel.SelectQuery{Fields: []string{"id", "name"}, OnlyDistinct: true},
		},
		{
			result:      "SELECT COUNT(*) AS `count`",
			selectQuery: rel.SelectQuery{Fields: []string{"COUNT(*) AS count"}},
		},
		{
			result:      "SELECT COUNT(`transactions`.*) AS `count`",
			selectQuery: rel.SelectQuery{Fields: []string{"COUNT(transactions.*) AS count"}},
		},
		{
			result:      "SELECT SUM(`transactions`.`total`) AS `total`",
			selectQuery: rel.SelectQuery{Fields: []string{"SUM(transactions.total) AS total"}},
		},
		{
			table:       "transactions",
			result:      "SELECT `transactions`.*",
			selectQuery: rel.SelectQuery{Fields: []string{"*"}},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteSelect(&buffer, test.table, test.selectQuery)
			assert.Equal(t, test.result, buffer.String())
		})
	}
}

func TestQuery_WriteFrom(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
		buffer        = bufferFactory.Create()
	)

	queryBuilder.WriteFrom(&buffer, "users")
	assert.Equal(t, " FROM `users`", buffer.String())
}

func TestQuery_WriteJoin(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		query  rel.Query
	}{
		{
			query: rel.From("transactions"),
		},
		{
			result: " JOIN `users` ON `transactions`.`user_id`=`users`.`id`",
			query:  rel.From("transactions").Join("users"),
		},
		{
			result: " JOIN `users` ON `users`.`id`=`transactions`.`user_id`",
			query:  rel.From("transactions").JoinOn("users", "users.id", "transactions.user_id"),
		},
		{
			result: " INNER JOIN `users` ON `users`.`id`=`transactions`.`user_id`",
			query:  rel.From("transactions").JoinWith("INNER JOIN", "users", "users.id", "transactions.user_id"),
		},
		{
			result: " JOIN `users` ON `users`.`id`=`transactions`.`user_id` JOIN `payments` ON `payments`.`id`=`transactions`.`payment_id`",
			query: rel.From("transactions").JoinOn("users", "users.id", "transactions.user_id").
				JoinOn("payments", "payments.id", "transactions.payment_id"),
		},
		{
			result: " JOIN `users` ON `users`.`id`=`transactions`.`user_id` JOIN `payments` ON `payments`.`id`=`transactions`.`payment_id` AND `payments`.`deleted`=?",
			query: rel.From("transactions").JoinOn("users", "users.id", "transactions.user_id").
				JoinOn("payments", "payments.id", "transactions.payment_id", rel.Eq("deleted", false)),
			args: []any{false},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteJoin(&buffer, "transactions", rel.Build("", test.query).JoinQuery)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteWhere(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: " WHERE `field`=?",
			args:   []any{"value"},
			filter: where.Eq("field", "value"),
		},
		{
			result: " WHERE (`field1`=? AND `field2`=?)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2"),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteWhere(&buffer, test.table, test.filter)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteWhere_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: " WHERE \"field\"=$1",
			args:   []any{"value"},
			filter: where.Eq("field", "value"),
		},
		{
			result: " WHERE (\"field1\"=$1 AND \"field2\"=$2)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2"),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteWhere(&buffer, test.table, test.filter)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteWhere_SubQuery(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: " WHERE `field`=ANY(SELECT `table2`.`field1` FROM `table2` WHERE `table2`.`type`=?)",
			args:   []any{"value"},
			filter: where.Eq("field", rel.Any(
				rel.Select("field1").From("table2").Where(where.Eq("type", "value")),
			)),
		},
		{
			result: " WHERE `field`=(SELECT `table2`.`field1` FROM `table2` WHERE `table2`.`type`=?)",
			args:   []any{"value"},
			filter: where.Eq("field",
				rel.Select("field1").From("table2").Where(where.Eq("type", "value")),
			),
		},
		{
			result: " WHERE `field1` IN (SELECT `table2`.`field2` FROM `table2` WHERE `table2`.`field3` IN (?,?))",
			args:   []any{"value1", "value2"},
			filter: where.In("field1", rel.Select("field2").From("table2").Where(
				where.InString("field3", []string{"value1", "value2"}),
			)),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteWhere(&buffer, test.table, test.filter)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteWhere_SubQuery_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: " WHERE \"field1\"=ANY(SELECT \"table2\".\"field2\" FROM \"table2\" WHERE \"table2\".\"type\"=$1)",
			args:   []any{"value"},
			filter: where.Eq("field1", rel.Any(
				rel.Select("field2").From("table2").Where(where.Eq("type", "value")),
			)),
		},
		{
			result: " WHERE \"field1\"=(SELECT \"table2\".\"field2\" FROM \"table2\" WHERE \"table2\".\"type\"=$1)",
			args:   []any{"value"},
			filter: where.Eq("field1",
				rel.Select("field2").From("table2").Where(where.Eq("type", "value")),
			),
		},
		{
			result: " WHERE \"field1\" IN (SELECT \"table2\".\"field2\" FROM \"table2\" WHERE \"table2\".\"field3\" IN ($1,$2))",
			args:   []any{"value1", "value2"},
			filter: where.In("field1",
				rel.Select("field2").From("table2").Where(
					where.InString("field3", []string{"value1", "value2"}),
				),
			),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteWhere(&buffer, test.table, test.filter)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteGroupBy(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	t.Run("single field", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteGroupBy(&buffer, "", []string{"city"})
		assert.Equal(t, " GROUP BY `city`", buffer.String())
	})

	t.Run("multiple fields", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteGroupBy(&buffer, "", []string{"city", "nation"})
		assert.Equal(t, " GROUP BY `city`,`nation`", buffer.String())
	})

	t.Run("multiple fields with table", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteGroupBy(&buffer, "table", []string{"city", "table2.nation"})
		assert.Equal(t, " GROUP BY `table`.`city`,`table2`.`nation`", buffer.String())
	})
}

func TestQuery_WriteHaving(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: " HAVING `field`=?",
			args:   []any{"value"},
			filter: where.Eq("field", "value"),
		},
		{
			result: " HAVING (`field1`=? AND `field2`=?)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2"),
		},
		{
			result: "",
			filter: where.And(),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteHaving(&buffer, test.table, test.filter)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteHaving_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: " HAVING \"field\"=$1",
			args:   []any{"value"},
			filter: where.Eq("field", "value"),
		},
		{
			result: " HAVING (\"field1\"=$1 AND \"field2\"=$2)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2"),
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			queryBuilder.WriteHaving(&buffer, test.table, test.filter)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestQuery_WriteOrderBy(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	t.Run("single sort", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteOrderBy(&buffer, "", []rel.SortQuery{sort.Asc("name")})
		assert.Equal(t, " ORDER BY `name` ASC", buffer.String())
	})

	t.Run("multiple sorts", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteOrderBy(&buffer, "", []rel.SortQuery{sort.Asc("name"), sort.Desc("created_at")})
		assert.Equal(t, " ORDER BY `name` ASC, `created_at` DESC", buffer.String())
	})

	t.Run("multiple sorts with table", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteOrderBy(&buffer, "table", []rel.SortQuery{sort.Asc("name"), sort.Desc("table2.created_at")})
		assert.Equal(t, " ORDER BY `table`.`name` ASC, `table2`.`created_at` DESC", buffer.String())
	})
}

func TestQuery_WriteLimitOffset(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		queryBuilder  = Query{BufferFactory: bufferFactory}
	)

	t.Run("limit", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteLimitOffset(&buffer, 10, 0)
		assert.Equal(t, " LIMIT 10", buffer.String())
	})

	t.Run("limit and offset", func(t *testing.T) {
		buffer := bufferFactory.Create()
		queryBuilder.WriteLimitOffset(&buffer, 10, 10)
		assert.Equal(t, " LIMIT 10 OFFSET 10", buffer.String())
	})
}
