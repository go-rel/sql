package sql

import (
	"github.com/go-rel/rel"
)

// InstrumentationAdapter component.
type InstrumentationAdapter struct {
	Instrumenter rel.Instrumenter
}

// Instrumentation set instrumenter for this adapter.
func (ia *InstrumentationAdapter) Instrumentation(instrumenter rel.Instrumenter) {
	ia.Instrumenter = instrumenter
}

// NewInstrumentationAdapter component.
func NewInstrumentationAdapter() *InstrumentationAdapter {
	return &InstrumentationAdapter{}
}
