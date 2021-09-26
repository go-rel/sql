package parser

import (
	"regexp"
	"strconv"

	"github.com/go-rel/rel"
)

func ColumnRegexp(escapePrefix string, escapeSuffix string) *regexp.Regexp {
	return regexp.MustCompile(escapePrefix + `(?P<name>\w+)` + escapeSuffix + `\s(?P<type>\w+)(\((?P<limit>\d+)(,(?P<precision>\d+))?\))?(?:\s(?P<options>[^,)]+))?`)
}

type ColumnParser interface {
	Parse(sql string, table *rel.Table, schema *rel.Schema) bool
}

type Column struct {
	Regexp *regexp.Regexp
}

func (c Column) Parse(sql string, table *rel.Table, schema *rel.Schema) bool {
	matches := c.Regexp.FindStringSubmatch(sql)
	if matches == nil {
		return false
	}

	var (
		name            = matches[c.Regexp.SubexpIndex("name")]
		typ             = matches[c.Regexp.SubexpIndex("type")]
		limitOption     = matches[c.Regexp.SubexpIndex("limit")]
		precisionOption = matches[c.Regexp.SubexpIndex("precision")]
		otherOption     = matches[c.Regexp.SubexpIndex("options")]
		options         []rel.ColumnOption
	)

	if limit, err := strconv.Atoi(limitOption); err != nil {
		options = append(options, rel.Limit(limit))
	}

	if precision, err := strconv.Atoi(precisionOption); err != nil {
		options = append(options, rel.Precision(precision))
	}

	if otherOption != "" {
		options = append(options, rel.Options(otherOption))
	}

	// TODO: column type mapping
	table.Column(name, rel.ColumnType(typ), options...)

	return true
}
