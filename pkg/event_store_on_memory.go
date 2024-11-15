package pkg

import (
	"context"
)

const initialVersion uint64 = 1

// EventStoreOnMemory is the memory implementation of EventStore.
type EventStoreOnMemory struct {
	events    map[string][]Event
	snapshots map[string]Aggregate
}

// NewEventStoreOnMemory is the constructor of EventStoreOnMemory.
//
// The returned value is the pointer to EventStoreOnMemory.
func NewEventStoreOnMemory() EventStore {
	return &EventStoreOnMemory{events: make(map[string][]Event), snapshots: make(map[string]Aggregate)}
}

func (es *EventStoreOnMemory) GetLatestSnapshotById(_ context.Context, aggregateId AggregateId) (*AggregateResult, error) {
	snapshot := es.snapshots[aggregateId.AsString()]
	if snapshot != nil {
		return &AggregateResult{aggregate: snapshot}, nil
	}
	return &AggregateResult{}, nil
}

func (es *EventStoreOnMemory) GetEventsByIdSinceSeqNr(_ context.Context, aggregateId AggregateId, seqNr uint64) ([]Event, error) {
	result := make([]Event, 0)
	for _, event := range es.events[aggregateId.AsString()] {
		if event.GetSeqNr() >= seqNr {
			result = append(result, event)
		}
	}

	return result, nil
}

func (es *EventStoreOnMemory) PersistEvent(_ context.Context, event Event, version uint64) error {
	if event.IsCreated() {
		panic("event is created")
	}

	aggregateId := event.GetAggregateId().AsString()
	if es.snapshots[aggregateId].GetVersion() != version {
		return NewOptimisticLockError("Transaction write was canceled due to conditional check failure", nil)
	}
	newVersion := es.snapshots[aggregateId].GetVersion() + 1
	es.events[aggregateId] = append(es.events[aggregateId], event)
	snapshot := es.snapshots[aggregateId]
	snapshot = snapshot.WithVersion(newVersion)
	es.snapshots[aggregateId] = snapshot

	return nil
}

func (es *EventStoreOnMemory) PersistEventAndSnapshot(_ context.Context, event Event, aggregate Aggregate) error {
	aggregateId := event.GetAggregateId().AsString()
	newVersion := initialVersion
	if !event.IsCreated() {
		version := es.snapshots[aggregateId].GetVersion()
		if version != aggregate.GetVersion() {
			return NewOptimisticLockError("Transaction write was canceled due to conditional check failure", nil)
		}
		newVersion = es.snapshots[aggregateId].GetVersion() + 1
	}

	es.events[aggregateId] = append(es.events[aggregateId], event)
	es.snapshots[aggregateId] = aggregate.WithVersion(newVersion)
	return nil
}
