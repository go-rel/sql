package parser

import (
	"regexp"
	"strings"

	"github.com/go-rel/rel"
)

// TableData extracted from sql string.
type TableData struct {
	Name        string
	Definitions []string
	Options     string
}

// Table sql parser.
type Table struct {
	Regexp       *regexp.Regexp
	ColumnParser ColumnParser
}

// Parse sql string as schema.
func (t Table) Parse(sql string, schema *rel.Schema) {
	var (
		tableData = t.Extract(sql)
	)

	schema.CreateTable(tableData.Name, func(table *rel.Table) {
		for _, definition := range tableData.Definitions {
			t.ColumnParser.Parse(strings.TrimSpace(definition), table, schema)
		}
	}, rel.Options(tableData.Options))
}

func (t Table) Extract(sql string) TableData {
	tableMatches := t.Regexp.FindStringSubmatch(strings.TrimSuffix(sql, ";"))
	if tableMatches == nil {
		panic("rel: error when parsing table sql: \n" + sql)
	}

	return TableData{
		Name: tableMatches[t.Regexp.SubexpIndex("name")]
		Definitions: strings.Split(tableMatches[t.Regexp.SubexpIndex("definitions")], ",")
		Options: tableMatches[t.Regexp.SubexpIndex("options")]
	}
}

// TableRegexp for capturing table name, definitions and options.
func TableRegexp(escapePrefix string, escapeSuffix string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)CREATE TABLE ` + escapePrefix + `(?P<name>\w+)` + escapeSuffix + `\s?\((?P<definitions>[\S\s]+)\)\s?(?P<options>[\S\s]*)`)
}
