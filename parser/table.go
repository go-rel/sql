package parser

import (
	"regexp"
	"strings"

	"github.com/go-rel/rel"
)

// TableRegexp for capturing table name, definitions and options.
func TableRegexp(escapePrefix string, escapeSuffix string) *regexp.Regexp {
	return regexp.MustCompile(`CREATE TABLE ` + escapePrefix + `(?P<name>\w+)` + escapeSuffix + `\s?\((?P<definitions>[\S\s]+)\)\s?(?P<options>[\S\s]*)`)
}

type Table struct {
	Regexp       *regexp.Regexp
	ColumnParser ColumnParser
}

func (t Table) Parse(sql string, schema *rel.Schema) {
	var (
		tableMatches     = t.Regexp.FindStringSubmatch(strings.TrimSuffix(sql, ";"))
		tableName        = tableMatches[t.Regexp.SubexpIndex("name")]
		tableDefinitions = strings.Split(tableMatches[t.Regexp.SubexpIndex("definitions")], ",")
		tableOptions     = tableMatches[t.Regexp.SubexpIndex("options")]
	)

	schema.CreateTable(tableName, func(table *rel.Table) {
		for _, definition := range tableDefinitions {
			t.ColumnParser.Parse(strings.TrimSpace(definition), table, schema)
		}
	}, rel.Options(tableOptions))
}
