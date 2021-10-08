package builder

import (
	"database/sql/driver"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuffer_escape(t *testing.T) {
	buffer := Buffer{Quoter: &SqlQuoter{IDPrefix: "[", IDSuffix: "]"}}

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

func TestBuffer_Arguments(t *testing.T) {
	var (
		buffer           = Buffer{Quoter: &SqlQuoter{IDPrefix: "[", IDSuffix: "]"}}
		initialArguments = []interface{}{1}
	)

	assert.Nil(t, buffer.Arguments())

	buffer.AddArguments(initialArguments...)
	assert.Equal(t, initialArguments, buffer.Arguments())

	buffer.AddArguments(2)
	assert.Equal(t, []interface{}{1, 2}, buffer.Arguments())

	buffer.Reset()
	assert.Nil(t, buffer.Arguments())
}

type customType struct {
	val string
}

func (c customType) String() string {
	return c.val
}

type customValuerType struct {
	val string
}

func (c customValuerType) Value() (driver.Value, error) {
	return c.val, nil
}

func TestBuffer_InlineValue(t *testing.T) {
	bf := BufferFactory{InlineValues: true, BoolTrueValue: "1", BoolFalseValue: "0", Quoter: &SqlQuoter{ValueQuote: "'", ValueQuoteEscapeChar: "'"}}

	tests := []struct {
		value  interface{}
		result string
	}{
		{
			value:  true,
			result: "1",
		},
		{
			value:  false,
			result: "0",
		},
		{
			value:  nil,
			result: "NULL",
		},
		{
			value:  122,
			result: "122",
		},
		{
			value:  float32(1.24),
			result: "1.24",
		},
		{
			value:  float64(1.23),
			result: "1.23",
		},
		{
			value:  uint64(123),
			result: "123",
		},
		{
			value:  "Test",
			result: "'Test'",
		},
		{
			value:  "Test's",
			result: "'Test''s'",
		},
		{
			value:  []byte("Test's"),
			result: "'Test''s'",
		},
		{
			value:  customType{"test"},
			result: "test",
		},
		{
			value:  customValuerType{"test"},
			result: "'test'",
		},
	}
	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			buffer := bf.Create()
			buffer.WriteValue(test.value)

			assert.Equal(t, test.result, buffer.String())
		})
	}
}
