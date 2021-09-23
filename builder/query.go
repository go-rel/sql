package builder

import (
	"strconv"
	"strings"

	"github.com/go-rel/rel"
)

// Query builder.
type Query struct {
	Name   Name
	Filter Filter
}

// Build SQL string and it arguments.
func (q Query) Build(query rel.Query) (string, []interface{}) {
	var buffer Buffer
	q.Write(&buffer, query)
	buffer.WriteString(";")

	return buffer.String(), buffer.Arguments()
}

// Write SQL to buffer.
func (q Query) Write(buffer *Buffer, query rel.Query) {
	if query.SQLQuery.Statement != "" {
		buffer.WriteString(query.SQLQuery.Statement)
		buffer.AddArguments(query.SQLQuery.Values...)
		return
	}

	q.BuildSelect(buffer, query.SelectQuery)
	q.BuildQuery(buffer, query)
}

// BuildSelect SQL to buffer.
func (q Query) BuildSelect(buffer *Buffer, selectQuery rel.SelectQuery) {
	if len(selectQuery.Fields) == 0 {
		if selectQuery.OnlyDistinct {
			buffer.WriteString("SELECT DISTINCT *")
			return
		}
		buffer.WriteString("SELECT *")
		return
	}

	buffer.WriteString("SELECT ")

	if selectQuery.OnlyDistinct {
		buffer.WriteString("DISTINCT ")
	}

	l := len(selectQuery.Fields) - 1
	for i, f := range selectQuery.Fields {
		buffer.WriteString(q.Name.Build(f))

		if i < l {
			buffer.WriteByte(',')
		}
	}
}

// BuildQuery SQL to buffer.
func (q Query) BuildQuery(buffer *Buffer, query rel.Query) {
	q.BuildFrom(buffer, query.Table)
	q.BuildJoin(buffer, query.Table, query.JoinQuery)
	q.BuildWhere(buffer, query.WhereQuery)

	if len(query.GroupQuery.Fields) > 0 {
		q.BuildGroupBy(buffer, query.GroupQuery.Fields)
		q.BuildHaving(buffer, query.GroupQuery.Filter)
	}

	q.BuildOrderBy(buffer, query.SortQuery)
	q.BuildLimitOffset(buffer, query.LimitQuery, query.OffsetQuery)

	if query.LockQuery != "" {
		buffer.WriteByte(' ')
		buffer.WriteString(string(query.LockQuery))
	}
}

// BuildFrom SQL to buffer.
func (q Query) BuildFrom(buffer *Buffer, table string) {
	buffer.WriteString(" FROM ")
	buffer.WriteString(q.Name.Build(table))
}

// BuildJoin SQL to buffer.
func (q Query) BuildJoin(buffer *Buffer, table string, joins []rel.JoinQuery) {
	if len(joins) == 0 {
		return
	}

	for _, join := range joins {
		var (
			from = q.Name.Build(join.From)
			to   = q.Name.Build(join.To)
		)

		// TODO: move this to core functionality, and infer join condition using assoc data.
		if join.Arguments == nil && (join.From == "" || join.To == "") {
			from = q.Name.Build(table + "." + strings.TrimSuffix(join.Table, "s") + "_id")
			to = q.Name.Build(join.Table + ".id")
		}

		buffer.WriteByte(' ')
		buffer.WriteString(join.Mode)
		buffer.WriteByte(' ')

		if join.Table != "" {
			buffer.WriteString(q.Name.Build(join.Table))
			buffer.WriteString(" ON ")
			buffer.WriteString(from)
			buffer.WriteString("=")
			buffer.WriteString(to)
		}

		buffer.AddArguments(join.Arguments...)
	}
}

// BuildWhere SQL to buffer.
func (q Query) BuildWhere(buffer *Buffer, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" WHERE ")
	q.Filter.Write(buffer, filter, q)
}

// BuildGroupBy SQL to buffer.
func (q Query) BuildGroupBy(buffer *Buffer, fields []string) {
	buffer.WriteString(" GROUP BY ")

	l := len(fields) - 1
	for i, f := range fields {
		buffer.WriteString(q.Name.Build(f))

		if i < l {
			buffer.WriteByte(',')
		}
	}
}

// BuildHaving SQL to buffer.
func (q Query) BuildHaving(buffer *Buffer, filter rel.FilterQuery) {
	if filter.None() {
		return
	}

	buffer.WriteString(" HAVING ")
	q.Filter.Write(buffer, filter, q)
}

// BuildOrderBy SQL to buffer.
func (q Query) BuildOrderBy(buffer *Buffer, orders []rel.SortQuery) {
	var (
		length = len(orders)
	)

	if length == 0 {
		return
	}

	buffer.WriteString(" ORDER BY ")
	for i, order := range orders {
		buffer.WriteString(q.Name.Build(order.Field))

		if order.Asc() {
			buffer.WriteString(" ASC")
		} else {
			buffer.WriteString(" DESC")
		}

		if i < length-1 {
			buffer.WriteByte(',')
		}
	}
}

// BuildLimitOffset SQL to buffer.
func (q Query) BuildLimitOffset(buffer *Buffer, limit rel.Limit, offset rel.Offset) {
	if limit > 0 {
		buffer.WriteString(" LIMIT ")
		buffer.WriteString(strconv.Itoa(int(limit)))

		if offset > 0 {
			buffer.WriteString(" OFFSET ")
			buffer.WriteString(strconv.Itoa(int(offset)))
		}
	}
}
