package pkg

import (
	"context"
)

// EventStore is the interface for persisting events and snapshots.
type EventStore interface {
	// GetLatestSnapshotById returns the latest snapshot of the aggregate.
	GetLatestSnapshotById(ctx context.Context, aggregateId AggregateId) (*AggregateResult, error)
	// GetEventsByIdSinceSeqNr returns the events of the aggregate since the specified sequence number.
	GetEventsByIdSinceSeqNr(ctx context.Context, aggregateId AggregateId, seqNr uint64) ([]Event, error)
	// PersistEvent persists the event.
	PersistEvent(ctx context.Context, event Event, version uint64) error
	// PersistEventAndSnapshot persists the event and the snapshot.
	PersistEventAndSnapshot(ctx context.Context, event Event, aggregate Aggregate) error
}

// AggregateId is the interface that represents the aggregate id of DDD.
type AggregateId interface {
	String() string

	// GetTypeName returns the type name of the aggregate id.
	GetTypeName() string

	// GetValue returns the value of the aggregate id.
	GetValue() string

	// AsString returns the string representation of the aggregate id.
	//
	// The string representation is {TypeName}-{Value}.
	AsString() string
}

// Event is the interface that represents the domain event of DDD.
type Event interface {
	String() string

	// GetId returns the id of the event.
	GetId() string

	// GetTypeName returns the type name of the event.
	GetTypeName() string

	// GetAggregateId returns the aggregate id of the event.
	GetAggregateId() AggregateId

	// GetSeqNr returns the sequence number of the event.
	GetSeqNr() uint64

	// IsCreated returns true if the event is created.
	IsCreated() bool

	// GetOccurredAt returns the occurred at of the event.
	GetOccurredAt() uint64
}

// Aggregate is the interface that represents the aggregate of DDD.
type Aggregate interface {
	String() string

	// GetId returns the id of the aggregate.
	GetId() AggregateId

	// GetSeqNr returns the sequence number of the aggregate.
	GetSeqNr() uint64

	// GetVersion returns the version of the aggregate.
	GetVersion() uint64

	// WithVersion returns a new aggregate with the specified version.
	WithVersion(version uint64) Aggregate
}
