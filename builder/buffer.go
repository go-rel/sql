package builder

import (
	"database/sql/driver"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"sync"
)

// UnescapeCharacter disable field escaping when it starts with this character.
var UnescapeCharacter byte = '^'

var escapeCache sync.Map

type escapeCacheKey struct {
	value  string
	quoter Quoter
}

// Buffer is used to build query string.
type Buffer struct {
	strings.Builder
	Quoter              Quoter
	ValueConverter      driver.ValueConverter
	ArgumentPlaceholder string
	ArgumentOrdinal     bool
	InlineValues        bool
	valueCount          int
	arguments           []interface{}
}

// WriteValue query placeholder and append value to argument.
func (b *Buffer) WriteValue(value interface{}) {
	if !b.InlineValues {
		b.WritePlaceholder()
		b.arguments = append(b.arguments, value)
		return
	}

	// Detect float bits to not lose precision after converting to float64
	var floatBits = 64
	if reflect.ValueOf(value).Kind() == reflect.Float32 {
		floatBits = 32
	}

	if b.ValueConverter != nil {
		if v, err := b.ValueConverter.ConvertValue(value); err != nil {
			log.Printf("[WARN] unsupported inline value %v", value)
			return
		} else {
			value = v
		}
	}

	switch v := value.(type) {
	case string:
		b.WriteString(b.Quoter.Value(v))
		return
	case []byte:
		b.WriteString(b.Quoter.Value(string(v)))
		return
	}

	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		b.WriteString(strconv.FormatInt(rv.Int(), 10))
		return
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		b.WriteString(strconv.FormatUint(rv.Uint(), 10))
		return
	case reflect.Float32, reflect.Float64:
		b.WriteString(strconv.FormatFloat(rv.Float(), 'g', -1, floatBits))
		return
	case reflect.Bool:
		b.WriteString(strconv.FormatBool(rv.Bool()))
		return
	}
	b.WriteString(fmt.Sprintf("%v", value))
}

// WritePlaceholder without adding argument.
// argument can be added later using AddArguments function.
func (b *Buffer) WritePlaceholder() {
	b.valueCount++
	b.WriteString(b.ArgumentPlaceholder)
	if b.ArgumentOrdinal {
		b.WriteString(strconv.Itoa(b.valueCount))
	}
}

// WriteEscape string.
func (b *Buffer) WriteEscape(value string) {
	b.WriteString(b.escape(value))
}

func (b Buffer) escape(value string) string {
	if value == "*" {
		return value
	}

	key := escapeCacheKey{value: value, quoter: b.Quoter}
	escapedValue, ok := escapeCache.Load(key)
	if ok {
		return escapedValue.(string)
	}

	if len(value) > 0 && value[0] == UnescapeCharacter {
		escapedValue = value[1:]
	} else if i := strings.Index(strings.ToLower(value), " as "); i > -1 {
		escapedValue = b.escape(value[:i]) + " AS " + b.escape(value[i+4:])
	} else if start, end := strings.IndexRune(value, '('), strings.IndexRune(value, ')'); start >= 0 && end >= 0 && end > start {
		escapedValue = value[:start+1] + b.escape(value[start+1:end]) + value[end:]
	} else {
		parts := strings.Split(value, ".")
		for i, part := range parts {
			part = strings.TrimSpace(part)
			if part == "*" && i == len(parts)-1 {
				break
			}
			parts[i] = b.Quoter.ID(part)
		}
		escapedValue = strings.Join(parts, ".")
	}

	escapeCache.Store(key, escapedValue)
	return escapedValue.(string)
}

// AddArguments appends multiple arguments without writing placeholder query..
func (b *Buffer) AddArguments(args ...interface{}) {
	if b.arguments == nil {
		b.arguments = args
	} else {
		b.arguments = append(b.arguments, args...)
	}
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

// BufferFactory is used to create buffer based on shared settings.
type BufferFactory struct {
	Quoter              Quoter
	ValueConverter      driver.ValueConverter
	ArgumentPlaceholder string
	ArgumentOrdinal     bool
	InlineValues        bool
}

func (bf BufferFactory) Create() Buffer {
	conv := bf.ValueConverter
	if conv == nil {
		conv = driver.DefaultParameterConverter
	}
	return Buffer{
		Quoter:              bf.Quoter,
		ValueConverter:      conv,
		ArgumentPlaceholder: bf.ArgumentPlaceholder,
		ArgumentOrdinal:     bf.ArgumentOrdinal,
		InlineValues:        bf.InlineValues,
	}
}
