package builder

import (
	"github.com/go-rel/rel"
)

type OnConflict struct {
	Statement       string
	IgnoreStatement string
	UpdateStatement string
	TableQualifier  string
	SupportKey      bool
	UseValues       bool
}

func (oc OnConflict) Write(buffer *Buffer, fields []string, onConflict rel.OnConflict) {
	if onConflict.Keys == nil && onConflict.Fragment == "" {
		return
	}

	buffer.WriteByte(' ')
	buffer.WriteString(oc.Statement)
	oc.WriteKeys(buffer, onConflict)

	buffer.WriteByte(' ')
	switch {
	case onConflict.Ignore:
		buffer.WriteString(oc.IgnoreStatement)
	case onConflict.Replace:
		buffer.WriteString(oc.UpdateStatement)
		buffer.WriteByte(' ')
		oc.WriteReplace(buffer, fields)
	case onConflict.Fragment != "":
		buffer.WriteString(onConflict.Fragment)
		buffer.AddArguments(onConflict.FragmentArgs...)
	}
}

func (oc OnConflict) WriteMutates(buffer *Buffer, mutates map[string]rel.Mutate, onConflict rel.OnConflict) {
	var fields []string
	if onConflict.Replace {
		fields = make([]string, len(mutates))
		i := 0
		for field := range mutates {
			fields[i] = field
			i++
		}
	}
	oc.Write(buffer, fields, onConflict)
}

func (oc OnConflict) WriteKeys(buffer *Buffer, onConflict rel.OnConflict) {
	if !oc.SupportKey || len(onConflict.Keys) == 0 {
		return
	}

	buffer.WriteByte('(')
	for i := range onConflict.Keys {
		if i > 0 {
			buffer.WriteByte(',')
		}
		buffer.WriteEscape(onConflict.Keys[i])
	}
	buffer.WriteByte(')')
}

func (oc OnConflict) WriteReplace(buffer *Buffer, fields []string) {
	for i, field := range fields {
		if i > 0 {
			buffer.WriteByte(',')
		}

		buffer.WriteEscape(field)
		buffer.WriteByte('=')

		if oc.UseValues {
			buffer.WriteString("VALUES(")
			buffer.WriteEscape(field)
			buffer.WriteByte(')')
		} else {
			buffer.WriteField(oc.TableQualifier, field)
		}
		i++
	}
}
