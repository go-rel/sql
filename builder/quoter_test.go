package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlQuoter_ID(t *testing.T) {
	quoter := &SqlQuoter{IDPrefix: "[", IDSuffix: "]", IDSuffixEscapeChar: "]"}

	tests := []struct {
		field  string
		result string
	}{
		{
			field:  "count",
			result: "[count]",
		},
		{
			field:  "use]r",
			result: "[use]]r]",
		},
	}
	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				result = quoter.ID(test.field)
			)

			assert.Equal(t, test.result, result)
		})
	}
}

func TestSqlQuoter_Value(t *testing.T) {
	quoter := &SqlQuoter{ValueQuote: "'", ValueQuoteEscapeChar: "'"}

	tests := []struct {
		value  interface{}
		result string
		panic  bool
	}{
		{
			value:  "count",
			result: "'count'",
		},
		{
			value:  "'count'",
			result: "'''count'''",
		},
		{
			value:  1,
			result: "",
			panic:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			if test.panic {
				assert.PanicsWithValue(t, "unsupported value", func() {
					quoter.Value(test.value)
				})
				return
			}
			var (
				result = quoter.Value(test.value)
			)

			assert.Equal(t, test.result, result)
		})
	}
}
