package builder

import (
	"strconv"
	"strings"
)

// Buffer is used to build query string.
type Buffer struct {
	strings.Builder
	valueCount int
	arguments  []interface{}
}

// WriteValue query placeholder and append value to argument.
func (b *Buffer) WriteValue(value interface{}) {
	b.valueCount++
	b.WriteString("@p")
	b.WriteString(strconv.Itoa(b.valueCount))
	b.arguments = append(b.arguments, value)
}

// AddArguments appends multiple arguments without writing placeholder query..
func (b *Buffer) AddArguments(args ...interface{}) {
	b.arguments = append(b.arguments, args...)
}

func (b Buffer) Arguments() []interface{} {
	return b.arguments
}

// Reset buffer.
func (b *Buffer) Reset() {
	b.Builder.Reset()
	b.valueCount = 0
	b.arguments = nil
}
