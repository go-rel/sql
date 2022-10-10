package builder

import (
	"testing"

	"github.com/go-rel/rel"
	"github.com/go-rel/rel/where"
	"github.com/stretchr/testify/assert"
)

func TestFilter_Write(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "?", Quoter: Quote{IDPrefix: "`", IDSuffix: "`", IDSuffixEscapeChar: "`", ValueQuote: "'", ValueQuoteEscapeChar: "'"}}
		filterBuilder = Filter{}
		queryBuilder  = Query{BufferFactory: bufferFactory, Filter: filterBuilder}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			filter: where.And(),
		},
		{
			result: "`field`=?",
			args:   []any{"value"},
			filter: where.Eq("field", "value"),
		},
		{
			result: "`field`<>?",
			args:   []any{"value"},
			filter: where.Ne("field", "value"),
		},
		{
			result: "`field`<?",
			args:   []any{10},
			filter: where.Lt("field", 10),
		},
		{
			result: "`field`<=?",
			args:   []any{10},
			filter: where.Lte("field", 10),
		},
		{
			result: "`field`>?",
			args:   []any{10},
			filter: where.Gt("field", 10),
		},
		{
			result: "`field`>=?",
			args:   []any{10},
			filter: where.Gte("field", 10),
		},
		{
			result: "`field` IS NULL",
			filter: where.Nil("field"),
		},
		{
			result: "`field` IS NOT NULL",
			filter: where.NotNil("field"),
		},
		{
			result: "`field` IN (?)",
			args:   []any{"value1"},
			filter: where.In("field", "value1"),
		},
		{
			result: "`field` IN (?,?)",
			args:   []any{"value1", "value2"},
			filter: where.In("field", "value1", "value2"),
		},
		{
			result: "`field` IN (?,?,?)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.In("field", "value1", "value2", "value3"),
		},
		{
			result: "1=0",
			args:   nil,
			filter: where.In("field", []any{}...),
		},

		{
			result: "`field` NOT IN (?)",
			args:   []any{"value1"},
			filter: where.Nin("field", "value1"),
		},
		{
			result: "`field` NOT IN (?,?)",
			args:   []any{"value1", "value2"},
			filter: where.Nin("field", "value1", "value2"),
		},
		{
			result: "`field` NOT IN (?,?,?)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Nin("field", "value1", "value2", "value3"),
		},
		{
			result: "1=1",
			args:   nil,
			filter: where.Nin("field", []any{}...),
		},
		{
			result: "`field` LIKE ?",
			args:   []any{"%value%"},
			filter: where.Like("field", "%value%"),
		},
		{
			result: "`field` NOT LIKE ?",
			args:   []any{"%value%"},
			filter: where.NotLike("field", "%value%"),
		},
		{
			result: "FRAGMENT",
			filter: where.Fragment("FRAGMENT"),
		},
		{
			result: "(`field1`=? AND `field2`=?)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2"),
		},
		{
			result: "(`field1`=? AND `field2`=? AND `field3`=?)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2").AndEq("field3", "value3"),
		},
		{
			result: "(`field1`=? OR `field2`=?)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").OrEq("field2", "value2"),
		},
		{
			result: "(`field1`=? OR `field2`=? OR `field3`=?)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Eq("field1", "value1").OrEq("field2", "value2").OrEq("field3", "value3"),
		},
		{
			result: "NOT (`field1`=? AND `field2`=?)",
			args:   []any{"value1", "value2"},
			filter: where.Not(where.Eq("field1", "value1"), where.Eq("field2", "value2")),
		},
		{
			result: "NOT (`field1`=? AND `field2`=? AND `field3`=?)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Not(where.Eq("field1", "value1"), where.Eq("field2", "value2"), where.Eq("field3", "value3")),
		},
		{
			result: "((`field1`=? OR `field2`=?) AND `field3`=?)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.And(where.Or(where.Eq("field1", "value1"), where.Eq("field2", "value2")), where.Eq("field3", "value3")),
		},
		{
			result: "((`field1`=? OR `field2`=?) AND (`field3`=? OR `field4`=?))",
			args:   []any{"value1", "value2", "value3", "value4"},
			filter: where.And(where.Or(where.Eq("field1", "value1"), where.Eq("field2", "value2")), where.Or(where.Eq("field3", "value3"), where.Eq("field4", "value4"))),
		},
		{
			result: "(NOT (`field1`=? AND `field2`=?) AND NOT (`field3`=? OR `field4`=?))",
			args:   []any{"value1", "value2", "value3", "value4"},
			filter: where.And(where.Not(where.Eq("field1", "value1"), where.Eq("field2", "value2")), where.Not(where.Or(where.Eq("field3", "value3"), where.Eq("field4", "value4")))),
		},
		{
			result: "NOT (`field1`=? AND (`field2`=? OR `field3`=?) AND NOT (`field4`=? OR `field5`=?))",
			args:   []any{"value1", "value2", "value3", "value4", "value5"},
			filter: where.And(where.Not(where.Eq("field1", "value1"), where.Or(where.Eq("field2", "value2"), where.Eq("field3", "value3")), where.Not(where.Or(where.Eq("field4", "value4"), where.Eq("field5", "value5"))))),
		},
		{
			result: "((`field1` IN (?,?) OR `field2` NOT IN (?)) AND `field3` IN (?,?,?))",
			args:   []any{"value1", "value2", "value3", "value4", "value5", "value6"},
			filter: where.And(where.Or(where.In("field1", "value1", "value2"), where.Nin("field2", "value3")), where.In("field3", "value4", "value5", "value6")),
		},
		{
			result: "(`field1` LIKE ? AND `field2` NOT LIKE ?)",
			args:   []any{"%value1%", "%value2%"},
			filter: where.And(where.Like("field1", "%value1%"), where.NotLike("field2", "%value2%")),
		},
		{
			filter: rel.FilterQuery{Type: rel.FilterOp(9999)},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			filterBuilder.Write(&buffer, test.table, test.filter, queryBuilder)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}

func TestFilter_Write_ordinal(t *testing.T) {
	var (
		bufferFactory = BufferFactory{ArgumentPlaceholder: "$", ArgumentOrdinal: true, Quoter: Quote{IDPrefix: "\"", IDSuffix: "\""}}
		filterBuilder = Filter{}
		queryBuilder  = Query{BufferFactory: bufferFactory, Filter: filterBuilder}
	)

	tests := []struct {
		result string
		args   []any
		table  string
		filter rel.FilterQuery
	}{
		{
			result: "",
			filter: where.And(),
		},
		{
			result: "\"field\"=$1",
			args:   []any{"value"},
			filter: where.Eq("field", "value"),
		},
		{
			result: "\"field\"<>$1",
			args:   []any{"value"},
			filter: where.Ne("field", "value"),
		},
		{
			result: "\"field\"<$1",
			args:   []any{10},
			filter: where.Lt("field", 10),
		},
		{
			result: "\"field\"<=$1",
			args:   []any{10},
			filter: where.Lte("field", 10),
		},
		{
			result: "\"field\">$1",
			args:   []any{10},
			filter: where.Gt("field", 10),
		},
		{
			result: "\"field\">=$1",
			args:   []any{10},
			filter: where.Gte("field", 10),
		},
		{
			result: "\"field\" IS NULL",
			filter: where.Nil("field"),
		},
		{
			result: "\"field\" IS NOT NULL",
			filter: where.NotNil("field"),
		},
		{
			result: "\"field\" IN ($1)",
			args:   []any{"value1"},
			filter: where.In("field", "value1"),
		},
		{
			result: "\"field\" IN ($1,$2)",
			args:   []any{"value1", "value2"},
			filter: where.In("field", "value1", "value2"),
		},
		{
			result: "\"field\" IN ($1,$2,$3)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.In("field", "value1", "value2", "value3"),
		},
		{
			result: "\"field\" NOT IN ($1)",
			args:   []any{"value1"},
			filter: where.Nin("field", "value1"),
		},
		{
			result: "\"field\" NOT IN ($1,$2)",
			args:   []any{"value1", "value2"},
			filter: where.Nin("field", "value1", "value2"),
		},
		{
			result: "\"field\" NOT IN ($1,$2,$3)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Nin("field", "value1", "value2", "value3"),
		},
		{
			result: "\"field\" LIKE $1",
			args:   []any{"%value%"},
			filter: where.Like("field", "%value%"),
		},
		{
			result: "\"field\" NOT LIKE $1",
			args:   []any{"%value%"},
			filter: where.NotLike("field", "%value%"),
		},
		{
			result: "FRAGMENT",
			filter: where.Fragment("FRAGMENT"),
		},
		{
			result: "(\"field1\"=$1 AND \"field2\"=$2)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2"),
		},
		{
			result: "(\"field1\"=$1 AND \"field2\"=$2 AND \"field3\"=$3)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Eq("field1", "value1").AndEq("field2", "value2").AndEq("field3", "value3"),
		},
		{
			result: "(\"field1\"=$1 OR \"field2\"=$2)",
			args:   []any{"value1", "value2"},
			filter: where.Eq("field1", "value1").OrEq("field2", "value2"),
		},
		{
			result: "(\"field1\"=$1 OR \"field2\"=$2 OR \"field3\"=$3)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Eq("field1", "value1").OrEq("field2", "value2").OrEq("field3", "value3"),
		},
		{
			result: "NOT (\"field1\"=$1 AND \"field2\"=$2)",
			args:   []any{"value1", "value2"},
			filter: where.Not(where.Eq("field1", "value1"), where.Eq("field2", "value2")),
		},
		{
			result: "NOT (\"field1\"=$1 AND \"field2\"=$2 AND \"field3\"=$3)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.Not(where.Eq("field1", "value1"), where.Eq("field2", "value2"), where.Eq("field3", "value3")),
		},
		{
			result: "((\"field1\"=$1 OR \"field2\"=$2) AND \"field3\"=$3)",
			args:   []any{"value1", "value2", "value3"},
			filter: where.And(where.Or(where.Eq("field1", "value1"), where.Eq("field2", "value2")), where.Eq("field3", "value3")),
		},
		{
			result: "((\"field1\"=$1 OR \"field2\"=$2) AND (\"field3\"=$3 OR \"field4\"=$4))",
			args:   []any{"value1", "value2", "value3", "value4"},
			filter: where.And(where.Or(where.Eq("field1", "value1"), where.Eq("field2", "value2")), where.Or(where.Eq("field3", "value3"), where.Eq("field4", "value4"))),
		},
		{
			result: "(NOT (\"field1\"=$1 AND \"field2\"=$2) AND NOT (\"field3\"=$3 OR \"field4\"=$4))",
			args:   []any{"value1", "value2", "value3", "value4"},
			filter: where.And(where.Not(where.Eq("field1", "value1"), where.Eq("field2", "value2")), where.Not(where.Or(where.Eq("field3", "value3"), where.Eq("field4", "value4")))),
		},
		{
			result: "NOT (\"field1\"=$1 AND (\"field2\"=$2 OR \"field3\"=$3) AND NOT (\"field4\"=$4 OR \"field5\"=$5))",
			args:   []any{"value1", "value2", "value3", "value4", "value5"},
			filter: where.And(where.Not(where.Eq("field1", "value1"), where.Or(where.Eq("field2", "value2"), where.Eq("field3", "value3")), where.Not(where.Or(where.Eq("field4", "value4"), where.Eq("field5", "value5"))))),
		},
		{
			result: "((\"field1\" IN ($1,$2) OR \"field2\" NOT IN ($3)) AND \"field3\" IN ($4,$5,$6))",
			args:   []any{"value1", "value2", "value3", "value4", "value5", "value6"},
			filter: where.And(where.Or(where.In("field1", "value1", "value2"), where.Nin("field2", "value3")), where.In("field3", "value4", "value5", "value6")),
		},
		{
			result: "(\"field1\" LIKE $1 AND \"field2\" NOT LIKE $2)",
			args:   []any{"%value1%", "%value2%"},
			filter: where.And(where.Like("field1", "%value1%"), where.NotLike("field2", "%value2%")),
		},
		{
			filter: rel.FilterQuery{Type: rel.FilterOp(9999)},
		},
	}

	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				buffer = bufferFactory.Create()
			)

			filterBuilder.Write(&buffer, test.table, test.filter, queryBuilder)

			assert.Equal(t, test.result, buffer.String())
			assert.Equal(t, test.args, buffer.Arguments())
		})
	}
}
