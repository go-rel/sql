package builder

import (
	"strings"
	"sync"
)

// UnescapeCharacter disable field escaping when it starts with this character.
var UnescapeCharacter byte = '^'

var nameCache sync.Map

type nameCacheKey struct {
	field string
	name  Name
}

// Name builder.
type Name struct {
	Prefix string
	Suffix string
}

// Build return escaped field.
func (n Name) Build(field string) string {
	if n.Prefix == "" && n.Suffix == "" || field == "*" {
		return field
	}

	key := nameCacheKey{field: field, name: n}
	escapedField, ok := nameCache.Load(key)
	if ok {
		return escapedField.(string)
	}

	if len(field) > 0 && field[0] == UnescapeCharacter {
		escapedField = field[1:]
	} else if i := strings.Index(strings.ToLower(field), " as "); i > -1 {
		escapedField = n.Build(field[:i]) + " AS " + n.Build(field[i+4:])
	} else if start, end := strings.IndexRune(field, '('), strings.IndexRune(field, ')'); start >= 0 && end >= 0 && end > start {
		escapedField = field[:start+1] + n.Build(field[start+1:end]) + field[end:]
	} else if strings.HasSuffix(field, "*") {
		escapedField = n.Prefix + strings.Replace(field, ".", n.Suffix+".", 1)
	} else {
		escapedField = n.Prefix +
			strings.Replace(field, ".", n.Suffix+"."+n.Prefix, 1) +
			n.Suffix
	}

	nameCache.Store(key, escapedField)
	return escapedField.(string)
}
