package pkg

import (
	"context"
	"fmt"
)

type UserAccountRepository interface {
	StoreEvent(ctx context.Context, event Event, version uint64) error
	StoreEventAndSnapshot(ctx context.Context, event Event, aggregate Aggregate) error
	FindById(ctx context.Context, id AggregateId) (*UserAccount, error)
}

type userAccountRepository struct {
	eventStore EventStore
}

func NewUserAccountRepository(eventStore EventStore) *userAccountRepository {
	return &userAccountRepository{
		eventStore: eventStore,
	}
}

func (r *userAccountRepository) StoreEvent(ctx context.Context, event Event, version uint64) error {
	return r.eventStore.PersistEvent(ctx, event, version)
}

func (r *userAccountRepository) StoreEventAndSnapshot(ctx context.Context, event Event, aggregate Aggregate) error {
	return r.eventStore.PersistEventAndSnapshot(ctx, event, aggregate)
}

func (r *userAccountRepository) FindById(ctx context.Context, id AggregateId) (*UserAccount, error) {
	result, err := r.eventStore.GetLatestSnapshotById(ctx, id)
	if err != nil {
		return nil, err
	}
	if result.Empty() {
		return nil, fmt.Errorf("not found")
	}

	events, err := r.eventStore.GetEventsByIdSinceSeqNr(ctx, id, result.Aggregate().GetSeqNr()+1)
	if err != nil {
		return nil, err
	}
	return replayUserAccount(events, result.Aggregate().(*UserAccount)), nil
}
