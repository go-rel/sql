package builder

import (
	"database/sql/driver"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuffer_escape(t *testing.T) {
	buffer := Buffer{Quoter: Quote{IDPrefix: "[", IDSuffix: "]"}}

	tests := []struct {
		table  string
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
			table:  "ignored",
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
		{
			table:  "user",
			field:  "*",
			result: "[user].*",
		},
		{
			table:  "user.user",
			field:  "*",
			result: "[user].[user].*",
		},
		{
			table:  "user",
			field:  "address as home_address",
			result: "[user].[address] AS [home_address]",
		},
		{
			table:  "user",
			field:  "count(*) as count",
			result: "count([user].*) AS [count]",
		},
		{
			table:  "user",
			field:  "person.address as home_address",
			result: "[person].[address] AS [home_address]",
		},
		{
			table:  "user",
			field:  "person.address as person.address",
			result: "[person].[address] AS [person.address]",
		},
	}
	for _, test := range tests {
		t.Run(test.result, func(t *testing.T) {
			var (
				result = buffer.escape(test.table, test.field)
			)

			assert.Equal(t, test.result, result)
		})
	}
}

func TestBuffer_Arguments(t *testing.T) {
	var (
		buffer           = Buffer{Quoter: Quote{IDPrefix: "[", IDSuffix: "]"}}
		initialArguments = []any{1}
	)

	assert.Nil(t, buffer.Arguments())

	buffer.AddArguments(initialArguments...)
	assert.Equal(t, initialArguments, buffer.Arguments())

	buffer.AddArguments(2)
	assert.Equal(t, []any{1, 2}, buffer.Arguments())

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
	bf := BufferFactory{InlineValues: true, BoolTrueValue: "1", BoolFalseValue: "0", Quoter: Quote{ValueQuote: "'", ValueQuoteEscapeChar: "'"}}

	tests := []struct {
		value  any
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
			value:  time.Unix(1633934368, 0).UTC(),
			result: "'2021-10-11 06:39:28'",
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
