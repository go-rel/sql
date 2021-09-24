package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuffer_escape(t *testing.T) {
	buffer := Buffer{EscapePrefix: "[", EscapeSuffix: "]"}

	tests := []struct {
		field  string
		result string
	}{
		{
			field:  "count(*) as count",
			result: "count(*) AS [count]",
		},
		{
			field:  "user.address as home_address",
			result: "[user].[address] AS [home_address]",
		},
		{
			field:  "^FIELD([gender], \"male\") AS order",
			result: "FIELD([gender], \"male\") AS order",
		},
		{
			field:  "*",
			result: "*",
		},
		{
			field:  "user.*",
			result: "[user].*",
		},
	}
	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				result = buffer.escape(test.field)
			)

			assert.Equal(t, test.result, result)
		})
	}
}
