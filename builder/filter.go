package builder

import (
	"github.com/go-rel/rel"
)

// Filter builder.
type Filter struct {
	Name Name
}

// Write SQL to buffer.
func (f Filter) Write(buffer *Buffer, filter rel.FilterQuery, queryBuilder Query) {
	switch filter.Type {
	case rel.FilterAndOp:
		f.BuildLogical(buffer, "AND", filter.Inner, queryBuilder)
	case rel.FilterOrOp:
		f.BuildLogical(buffer, "OR", filter.Inner, queryBuilder)
	case rel.FilterNotOp:
		buffer.WriteString("NOT ")
		f.BuildLogical(buffer, "AND", filter.Inner, queryBuilder)
	case rel.FilterEqOp,
		rel.FilterNeOp,
		rel.FilterLtOp,
		rel.FilterLteOp,
		rel.FilterGtOp,
		rel.FilterGteOp:
		f.BuildComparison(buffer, filter, queryBuilder)
	case rel.FilterNilOp:
		buffer.WriteString(f.Name.Build(filter.Field))
		buffer.WriteString(" IS NULL")
	case rel.FilterNotNilOp:
		buffer.WriteString(f.Name.Build(filter.Field))
		buffer.WriteString(" IS NOT NULL")
	case rel.FilterInOp,
		rel.FilterNinOp:
		f.BuildInclusion(buffer, filter, queryBuilder)
	case rel.FilterLikeOp:
		buffer.WriteString(f.Name.Build(filter.Field))
		buffer.WriteString(" LIKE ")
		buffer.WriteValue(filter.Value)
	case rel.FilterNotLikeOp:
		buffer.WriteString(f.Name.Build(filter.Field))
		buffer.WriteString(" NOT LIKE ")
		buffer.WriteValue(filter.Value)
	case rel.FilterFragmentOp:
		buffer.WriteString(filter.Field)
		buffer.AddArguments(filter.Value.([]interface{})...)
	}
}

// BuildLogical SQL to buffer.
func (f Filter) BuildLogical(buffer *Buffer, op string, inner []rel.FilterQuery, queryBuilder Query) {
	var (
		length = len(inner)
	)

	if length > 1 {
		buffer.WriteByte('(')
	}

	for i, c := range inner {
		f.Write(buffer, c, queryBuilder)

		if i < length-1 {
			buffer.WriteByte(' ')
			buffer.WriteString(op)
			buffer.WriteByte(' ')
		}
	}

	if length > 1 {
		buffer.WriteByte(')')
	}
}

// BuildComparison SQL to buffer.
func (f Filter) BuildComparison(buffer *Buffer, filter rel.FilterQuery, queryBuilder Query) {
	buffer.WriteString(f.Name.Build(filter.Field))

	switch filter.Type {
	case rel.FilterEqOp:
		buffer.WriteByte('=')
	case rel.FilterNeOp:
		buffer.WriteString("<>")
	case rel.FilterLtOp:
		buffer.WriteByte('<')
	case rel.FilterLteOp:
		buffer.WriteString("<=")
	case rel.FilterGtOp:
		buffer.WriteByte('>')
	case rel.FilterGteOp:
		buffer.WriteString(">=")
	}

	switch v := filter.Value.(type) {
	case rel.SubQuery:
		// For warped sub-queries
		f.buildSubQuery(buffer, v, queryBuilder)
	case rel.Query:
		// For sub-queries without warp
		f.buildSubQuery(buffer, rel.SubQuery{Query: v}, queryBuilder)
	default:
		// For simple values
		buffer.WriteValue(filter.Value)
	}
}

// BuildInclusion SQL to buffer.
func (f Filter) BuildInclusion(buffer *Buffer, filter rel.FilterQuery, queryBuilder Query) {
	var (
		values = filter.Value.([]interface{})
	)

	buffer.WriteString(f.Name.Build(filter.Field))

	if filter.Type == rel.FilterInOp {
		buffer.WriteString(" IN ")
	} else {
		buffer.WriteString(" NOT IN ")
	}

	f.buildInclusionValues(buffer, values, queryBuilder)
}

func (f Filter) buildInclusionValues(buffer *Buffer, values []interface{}, queryBuilder Query) {
	if len(values) == 1 {
		if value, ok := values[0].(rel.Query); ok {
			f.buildSubQuery(buffer, rel.SubQuery{Query: value}, queryBuilder)
			return
		}
	}

	buffer.WriteByte('(')
	for i := 0; i < len(values); i++ {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteValue(values[i])
	}
	buffer.WriteByte(')')
}

func (f Filter) buildSubQuery(buffer *Buffer, sub rel.SubQuery, queryBuilder Query) {
	buffer.WriteString(sub.Prefix)
	buffer.WriteByte('(')
	queryBuilder.Write(buffer, sub.Query)
	buffer.WriteByte(')')
}
