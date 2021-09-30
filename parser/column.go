package parser

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"

	"github.com/go-rel/rel"
)

// ColumnParser interface.
type ColumnParser interface {
	Parse(sql string, table *rel.Table, schema *rel.Schema) bool
}

// ColumnData extracted from sql string.
type ColumnData struct {
	Name      string
	Type      rel.ColumnType
	Limit     int
	Precision int
	Unsigned  bool
	Required  bool
	Default   interface{}
	Unique    bool
	Options   string
}

// Column sql parser.
type Column struct {
	Regexp *regexp.Regexp
}

// Parse sql string as column.
func (c Column) Parse(sql string, table *rel.Table, schema *rel.Schema) bool {
	columnData := c.Extract(sql)
	if columnData.Name == "" {
		return false
	}

	c.MapColumn(&columnData)
	c.AddColumn(table, columnData)
	return true
}

func (c Column) Extract(sql string) ColumnData {
	matches := c.Regexp.FindStringSubmatch(sql)
	if matches == nil {
		return ColumnData{}
	}

	var (
		defaultValue interface{}
		defaultStr   = matches[c.Regexp.SubexpIndex("default_value")]
		limit, _     = strconv.Atoi(matches[c.Regexp.SubexpIndex("limit")])
		precision, _ = strconv.Atoi(matches[c.Regexp.SubexpIndex("precision")])
		index        = matches[c.Regexp.SubexpIndex("index")]
		unique       = strings.EqualFold(index, "UNIQUE")
		options      = matches[c.Regexp.SubexpIndex("options")]
	)

	if strings.EqualFold(defaultStr, "NULL") {
		json.Unmarshal([]byte(matches[c.Regexp.SubexpIndex("default_value")]), &defaultValue)
	}

	if !unique && index != "" {
		options += index + " " + options
	}

	return ColumnData{
		Name:      matches[c.Regexp.SubexpIndex("name")],
		Type:      rel.ColumnType(strings.ToUpper(matches[c.Regexp.SubexpIndex("type")])),
		Limit:     limit,
		Precision: precision,
		Unsigned:  strings.EqualFold(matches[c.Regexp.SubexpIndex("unsigned")], "UNSIGNED"),
		Required:  strings.EqualFold(matches[c.Regexp.SubexpIndex("nullable")], "NOT NULL"),
		Default:   defaultValue,
		Unique:    unique,
		Options:   options,
	}
}

func (c Column) MapColumn(columnData *ColumnData) {
	switch {
	case columnData.Type == "VARCHAR":
		columnData.Type = rel.String
	}
}

func (c Column) AddColumn(table *rel.Table, columnData ColumnData) {
	table.Column(columnData.Name, columnData.Type,
		rel.Limit(columnData.Limit),
		rel.Precision(columnData.Precision),
		rel.Unsigned(columnData.Unsigned),
		rel.Required(columnData.Required),
		rel.Default(columnData.Default),
		rel.Unique(columnData.Unique),
		rel.Options(columnData.Options),
	)
}

func ColumnRegexp(escapePrefix string, escapeSuffix string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)` +
		escapePrefix + `(?P<name>\w+)` + escapeSuffix + `\s(?P<type>\w+)` +
		`(\((?P<limit>\d+)(,(?P<precision>\d+))?\))?` +
		`(?:\s(?P<unsigned>UNSIGNED))?` +
		`(?:\s(?P<nullable>(?:NOT\s)?NULL))?` +
		`(?:\sDEFAULT\s(?P<default_value>(?:['"].+['"]|[\w\d]+)))?` +
		`(?:\s(?P<index>(?:AUTO_INCREMENT|UNIQUE|PRIMARY))(?:\sKEY)?)?` +
		`(?:\s(?P<options>[^,)]+))?.*`)
}
