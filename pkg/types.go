package pkg

// AggregateConverter is the function type that converts map[string]any to Aggregate.
type AggregateConverter func(map[string]any) (Aggregate, error)

// EventConverter is the function type that converts map[string]any to Event.
type EventConverter func(map[string]any) (Event, error)

// AggregateResult is the result of aggregate.
type AggregateResult struct {
	aggregate Aggregate
}

// Present returns true if the aggregate is not nil.
func (a *AggregateResult) Present() bool {
	return a.aggregate != nil
}

// Empty returns true if the aggregate is nil.
func (a *AggregateResult) Empty() bool {
	return !a.Present()
}

// Aggregate returns the aggregate.
func (a *AggregateResult) Aggregate() Aggregate {
	if a.Empty() {
		panic("aggregate is nil")
	}
	return a.aggregate
}

// EventSerializer is an interface that serializes and deserializes events.
type EventSerializer interface {
	// Serialize serializes the event.
	Serialize(event Event) ([]byte, error)
	// Deserialize deserializes the event.
	Deserialize(data []byte, eventMap *map[string]any) error
}

// SnapshotSerializer is an interface that serializes and deserializes snapshots.
type SnapshotSerializer interface {
	// Serialize serializes the aggregate.
	Serialize(aggregate Aggregate) ([]byte, error)
	// Deserialize deserializes the aggregate.
	Deserialize(data []byte, aggregateMap *map[string]any) error
}

// EventStoreBaseError is a base error of EventStore.
type EventStoreBaseError struct {
	// Message is a message of the error.
	Message string
	// Cause is a cause of the error.
	Cause error
}

// Error returns the message of the error.
func (e *EventStoreBaseError) Error() string {
	return e.Message
}

// OptimisticLockError is an error that occurs when the version of the aggregate does not match.
type OptimisticLockError struct {
	EventStoreBaseError
}

// NewOptimisticLockError is the constructor of OptimisticLockError.
func NewOptimisticLockError(message string, cause error) *OptimisticLockError {
	return &OptimisticLockError{EventStoreBaseError{message, cause}}
}

// SerializationError is the error type that occurs when serialization fails.
type SerializationError struct {
	EventStoreBaseError
}

// NewSerializationError is the constructor of SerializationError.
func NewSerializationError(message string, cause error) *SerializationError {
	return &SerializationError{EventStoreBaseError{message, cause}}
}

// DeserializationError is the error type that occurs when deserialization fails.
type DeserializationError struct {
	EventStoreBaseError
}

// NewDeserializationError is the constructor of DeserializationError.
func NewDeserializationError(message string, cause error) *DeserializationError {
	return &DeserializationError{EventStoreBaseError{message, cause}}
}

// IOError is the error type that occurs when IO fails.
type IOError struct {
	EventStoreBaseError
}

// NewIOError is the constructor of IOError.
func NewIOError(message string, cause error) *IOError {
	return &IOError{EventStoreBaseError{message, cause}}
}
