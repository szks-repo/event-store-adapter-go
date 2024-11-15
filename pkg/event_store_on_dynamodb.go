package pkg

import (
	"context"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// EventStoreOnDynamoDB is EventStore for DynamoDB.
type EventStoreOnDynamoDB struct {
	client               *dynamodb.Client
	journalTableName     string
	snapshotTableName    string
	journalAidIndexName  string
	snapshotAidIndexName string
	shardCount           uint64
	eventConverter       EventConverter
	snapshotConverter    AggregateConverter
	keepSnapshot         bool
	keepSnapshotCount    uint32
	deleteTtl            time.Duration
	keyResolver          KeyResolver
	eventSerializer      EventSerializer
	snapshotSerializer   SnapshotSerializer
}

// EventStoreOption is an option for EventStore.
type EventStoreOption func(*EventStoreOnDynamoDB) error

// WithKeepSnapshot sets whether or not to keep snapshots.
//
// - If you want to keep snapshots, specify true.
// - If you do not want to keep snapshots, specify false.
// - The default is false.
//
// # Parameters
// - keepSnapshot is whether or not to keep snapshots.
//
// # Returns
// - an EventStoreOption.
func WithKeepSnapshot(keepSnapshot bool) EventStoreOption {
	return func(es *EventStoreOnDynamoDB) error {
		es.keepSnapshot = keepSnapshot
		return nil
	}
}

// WithDeleteTtl sets the ttl for deletion snapshots.
//
// - If you want to delete snapshots, specify a time.Duration.
// - If you do not want to delete snapshots, specify math.MaxInt64.
// - The default is math.MaxInt64.
//
// # Parameters
// - deleteTtl is the time to live for deletion snapshots.
//
// # Returns
// - an EventStoreOption.
func WithDeleteTtl(deleteTtl time.Duration) EventStoreOption {
	return func(es *EventStoreOnDynamoDB) error {
		es.deleteTtl = deleteTtl
		return nil
	}
}

// WithKeepSnapshotCount sets a keep snapshot count.
//
// - If you want to keep snapshots, specify a keep snapshot count.
// - If you do not want to keep snapshots, specify math.MaxInt64.
// - The default is math.MaxInt64.
//
// # Parameters
// - keepSnapshotCount is a keep snapshot count.
//
// # Returns
// - an EventStoreOption.
func WithKeepSnapshotCount(keepSnapshotCount uint32) EventStoreOption {
	return func(es *EventStoreOnDynamoDB) error {
		es.keepSnapshotCount = keepSnapshotCount
		return nil
	}
}

// WithKeyResolver sets a key resolver.
//
// - If you want to change the key resolver, specify a KeyResolver.
// - The default is DefaultKeyResolver.
//
// # Parameters
// - keyResolver is a key resolver.
//
// # Returns
// - an EventStoreOption.
func WithKeyResolver(keyResolver KeyResolver) EventStoreOption {
	return func(es *EventStoreOnDynamoDB) error {
		es.keyResolver = keyResolver
		return nil
	}
}

// WithEventSerializer sets an event serializer.
//
// - If you want to change the event serializer, specify an EventSerializer.
// - The default is DefaultEventSerializer.
//
// # Parameters
// - eventSerializer is an event serializer.
//
// # Returns
// - an EventStoreOption.
func WithEventSerializer(eventSerializer EventSerializer) EventStoreOption {
	return func(es *EventStoreOnDynamoDB) error {
		es.eventSerializer = eventSerializer
		return nil
	}
}

// WithSnapshotSerializer sets a snapshot serializer.
//
// - If you want to change the snapshot serializer, specify a SnapshotSerializer.
// - The default is DefaultSnapshotSerializer.
//
// # Parameters
// - snapshotSerializer is a snapshot serializer.
//
// # Returns
// - an EventStoreOption.
func WithSnapshotSerializer(snapshotSerializer SnapshotSerializer) EventStoreOption {
	return func(es *EventStoreOnDynamoDB) error {
		es.snapshotSerializer = snapshotSerializer
		return nil
	}
}

// NewEventStoreOnDynamoDB returns a new EventStore.
//
// # Parameters
// - client is a DynamoDB client.
// - journalTableName is a journal table name.
// - snapshotTableName is a snapshot table name.
// - journalAidIndexName is a journal aggregateId index name.
// - snapshotAidIndexName is a snapshot aggregateId index name.
// - shardCount is a shard count.
// - eventConverter is a converter to convert a map to an event.
// - snapshotConverter is a converter to convert a map to an aggregate.
// - options is an EventStoreOption.
//
// # Returns
// - an EventStore
// - an error
func NewEventStoreOnDynamoDB(
	client *dynamodb.Client,
	journalTableName string,
	snapshotTableName string,
	journalAidIndexName string,
	snapshotAidIndexName string,
	shardCount uint64,
	eventConverter EventConverter,
	snapshotConverter AggregateConverter,
	options ...EventStoreOption,
) (EventStore, error) {
	if client == nil {
		return nil, errors.New("client is nil")
	}
	if journalTableName == "" {
		return nil, errors.New("journalTableName is empty")
	}
	if snapshotTableName == "" {
		return nil, errors.New("snapshotTableName is empty")
	}
	if journalAidIndexName == "" {
		return nil, errors.New("journalAidIndexName is empty")
	}
	if snapshotAidIndexName == "" {
		return nil, errors.New("snapshotAidIndexName is empty")
	}
	if shardCount == 0 {
		return nil, errors.New("shardCount is zero")
	}
	es := &EventStoreOnDynamoDB{
		client:               client,
		journalTableName:     journalTableName,
		snapshotTableName:    snapshotTableName,
		journalAidIndexName:  journalAidIndexName,
		snapshotAidIndexName: snapshotAidIndexName,
		shardCount:           shardCount,
		eventConverter:       eventConverter,
		snapshotConverter:    snapshotConverter,
		keepSnapshot:         false,
		keepSnapshotCount:    1,
		deleteTtl:            math.MaxInt64,
		keyResolver:          &DefaultKeyResolver{},
		eventSerializer:      &DefaultEventSerializer{},
		snapshotSerializer:   &DefaultSnapshotSerializer{},
	}
	for _, option := range options {
		if err := option(es); err != nil {
			return nil, err
		}
	}

	return es, nil
}

func (es *EventStoreOnDynamoDB) GetLatestSnapshotById(ctx context.Context, aggregateId AggregateId) (*AggregateResult, error) {
	if aggregateId == nil {
		panic("aggregateId is nil")
	}

	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr = :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.AsString()},
			":seq_nr": &types.AttributeValueMemberN{Value: "0"},
		},
		Limit: aws.Int32(1),
	}

	result, err := es.client.Query(ctx, request)
	if err != nil {
		return nil, NewIOError("Failed to GetLatestSnapshotById query", err)
	} else if len(result.Items) == 0 {
		return &AggregateResult{}, nil
	} else if len(result.Items) > 1 {
		panic("len(result.Items) > 1")
	}

	version, err := strconv.ParseUint(result.Items[0]["version"].(*types.AttributeValueMemberN).Value, 10, 64)
	if err != nil {
		return nil, NewDeserializationError("Failed to parse the version", err)
	}

	var aggregateMap map[string]any
	if err := es.snapshotSerializer.Deserialize(result.Items[0]["payload"].(*types.AttributeValueMemberB).Value, &aggregateMap); err != nil {
		return nil, err
	}

	aggregate, err := es.snapshotConverter(aggregateMap)
	if err != nil {
		return nil, NewDeserializationError("Failed to convert the snapshot", err)
	}
	return &AggregateResult{aggregate.WithVersion(version)}, nil
}

func (es *EventStoreOnDynamoDB) GetEventsByIdSinceSeqNr(ctx context.Context, aggregateId AggregateId, seqNr uint64) ([]Event, error) {
	if aggregateId == nil {
		panic("aggregateId is nil")
	}
	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.journalTableName),
		IndexName:              aws.String(es.journalAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr >= :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.AsString()},
			":seq_nr": &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)},
		},
	}
	result, err := es.client.Query(context.Background(), request)
	if err != nil {
		return nil, NewIOError("Failed to GetEventsByIdSinceSeqNr query", err)
	}

	events := make([]Event, 0, len(result.Items))
	for _, item := range result.Items {
		var eventMap map[string]any
		if err := es.eventSerializer.Deserialize(item["payload"].(*types.AttributeValueMemberB).Value, &eventMap); err != nil {
			return nil, err
		}

		event, err := es.eventConverter(eventMap)
		if err != nil {
			return nil, NewDeserializationError("Failed to convert the event", err)
		}
		events = append(events, event)
	}

	return events, nil
}

func (es *EventStoreOnDynamoDB) PersistEvent(ctx context.Context, event Event, version uint64) error {
	if event.IsCreated() {
		panic("event is created")
	}
	if err := es.updateEventAndSnapshotOpt(ctx, event, version, nil); err != nil {
		return err
	}
	if err := es.tryPurgeExcessSnapshots(ctx, event); err != nil {
		return err
	}

	return nil
}

func (es *EventStoreOnDynamoDB) PersistEventAndSnapshot(ctx context.Context, event Event, aggregate Aggregate) error {
	if event.IsCreated() {
		if err := es.createEventAndSnapshot(ctx, event, aggregate); err != nil {
			return err
		}
	} else {
		if err := es.updateEventAndSnapshotOpt(ctx, event, aggregate.GetVersion(), aggregate); err != nil {
			return err
		}
		if err := es.tryPurgeExcessSnapshots(ctx, event); err != nil {
			return err
		}
	}
	return nil
}

// putSnapshot returns a PutInput for snapshot.
//
// # Parameters
// - event is an event to store.
// - seqNr is a seqNr of the event.
// - aggregate is an aggregate to store.
//
// # Returns
// - a PutInput
// - an error
func (es *EventStoreOnDynamoDB) putSnapshot(event Event, seqNr uint64, aggregate Aggregate) (*types.Put, error) {
	if event == nil {
		return nil, errors.New("event is nil")
	}
	if aggregate == nil {
		return nil, errors.New("aggregate is nil")
	}

	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId(), seqNr)
	payload, err := es.snapshotSerializer.Serialize(aggregate)
	if err != nil {
		return nil, err
	}

	input := types.Put{
		TableName: aws.String(es.snapshotTableName),
		Item: map[string]types.AttributeValue{
			"pkey":    &types.AttributeValueMemberS{Value: pkey},
			"skey":    &types.AttributeValueMemberS{Value: skey},
			"aid":     &types.AttributeValueMemberS{Value: event.GetAggregateId().AsString()},
			"seq_nr":  &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)},
			"payload": &types.AttributeValueMemberB{Value: payload},
			"version": &types.AttributeValueMemberN{Value: "1"},
			"ttl":     &types.AttributeValueMemberN{Value: "0"},
		},
		ConditionExpression: aws.String("attribute_not_exists(pkey) AND attribute_not_exists(skey)"),
	}

	return &input, nil
}

// updateSnapshot returns an UpdateInput for snapshot.
//
// # Parameters
// - event is an event to store.
// - seqNr is a seqNr of the event.
// - version is a version of the aggregate.
// - aggregate is an aggregate to store.
//   - Required when event is created, otherwise you can choose whether or not to save a snapshot.
//
// # Returns
// - an UpdateInput
// - an error
func (es *EventStoreOnDynamoDB) updateSnapshot(event Event, seqNr uint64, version uint64, aggregate Aggregate) (*types.Update, error) {
	if event == nil {
		return nil, errors.New("event is nil")
	}

	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId(), seqNr)
	update := types.Update{
		TableName:        aws.String(es.snapshotTableName),
		UpdateExpression: aws.String("SET #version=:after_version"),
		Key: map[string]types.AttributeValue{
			"pkey": &types.AttributeValueMemberS{Value: pkey},
			"skey": &types.AttributeValueMemberS{Value: skey},
		},
		ExpressionAttributeNames: map[string]string{
			"#version": "version",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":before_version": &types.AttributeValueMemberN{Value: strconv.FormatUint(version, 10)},
			":after_version":  &types.AttributeValueMemberN{Value: strconv.FormatUint(version+1, 10)},
		},
		ConditionExpression: aws.String("#version=:before_version"),
	}
	if aggregate != nil {
		payload, err := es.snapshotSerializer.Serialize(aggregate)
		if err != nil {
			return nil, err
		}
		update.UpdateExpression = aws.String("SET #payload=:payload, #seq_nr=:seq_nr, #version=:after_version")
		update.ExpressionAttributeNames["#seq_nr"] = "seq_nr"
		update.ExpressionAttributeNames["#payload"] = "payload"
		update.ExpressionAttributeValues[":seq_nr"] = &types.AttributeValueMemberN{Value: strconv.FormatUint(seqNr, 10)}
		update.ExpressionAttributeValues[":payload"] = &types.AttributeValueMemberB{Value: payload}
	}

	return &update, nil
}

// putJournal returns a PutInput for journal.
//
// # Parameters
// - event is an event to store.
//
// # Returns
// - a PutInput
// - an error
func (es *EventStoreOnDynamoDB) putJournal(event Event) (*types.Put, error) {
	if event == nil {
		return nil, errors.New("event is nil")
	}

	pkey := es.keyResolver.ResolvePkey(event.GetAggregateId(), es.shardCount)
	skey := es.keyResolver.ResolveSkey(event.GetAggregateId(), event.GetSeqNr())
	payload, err := es.eventSerializer.Serialize(event)
	if err != nil {
		return nil, err
	}

	input := types.Put{
		TableName: aws.String(es.journalTableName),
		Item: map[string]types.AttributeValue{
			"pkey":        &types.AttributeValueMemberS{Value: pkey},
			"skey":        &types.AttributeValueMemberS{Value: skey},
			"aid":         &types.AttributeValueMemberS{Value: event.GetAggregateId().AsString()},
			"seq_nr":      &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetSeqNr(), 10)},
			"payload":     &types.AttributeValueMemberB{Value: payload},
			"occurred_at": &types.AttributeValueMemberN{Value: strconv.FormatUint(event.GetOccurredAt(), 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(pkey) AND attribute_not_exists(skey)"),
	}

	return &input, nil
}

// tryPurgeExcessSnapshots tries to purge excess snapshots.
//
// # Parameters
// - event is an event to store.
// # Returns
// - an error
func (es *EventStoreOnDynamoDB) tryPurgeExcessSnapshots(ctx context.Context, event Event) error {
	if es.keepSnapshot && es.keepSnapshotCount > 0 {
		if es.deleteTtl < math.MaxInt64 {
			if err := es.updateTtlOfExcessSnapshots(ctx, event.GetAggregateId()); err != nil {
				return err
			}
		} else {
			if err := es.deleteExcessSnapshots(ctx, event.GetAggregateId()); err != nil {
				return err
			}
		}
	}
	return nil
}

// updateEventAndSnapshotOpt updates the event and the snapshot.
//
// # Parameters
// - event is an event to store.
// - version is a version of the aggregate.
// - aggregate is an aggregate to store.
// # Returns
// - an error
func (es *EventStoreOnDynamoDB) updateEventAndSnapshotOpt(ctx context.Context, event Event, version uint64, aggregate Aggregate) error {
	if event == nil {
		panic("event is nil")
	}
	putJournal, err := es.putJournal(event)
	if err != nil {
		return err
	}
	updateSnapshot, err := es.updateSnapshot(event, 0, version, aggregate)
	if err != nil {
		return err
	}

	transactItems := []types.TransactWriteItem{
		{Update: updateSnapshot},
		{Put: putJournal},
	}
	if es.keepSnapshot && aggregate != nil {
		putSnapshot2, err := es.putSnapshot(event, aggregate.GetSeqNr(), aggregate)
		if err != nil {
			return err
		}
		transactItems = append(transactItems, types.TransactWriteItem{Put: putSnapshot2})
	}
	if _, err := es.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems}); err != nil {
		var t *types.TransactionCanceledException
		switch {
		case errors.As(err, &t):
			for _, reason := range t.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return NewOptimisticLockError("Transaction write was canceled due to conditional check failure", err)
				}
			}
			return NewIOError("Failed to transact write items due to non-conditional check failure", err)
		default:
			return NewIOError("Failed to transact write items", err)
		}
	}

	return nil
}

// createEventAndSnapshot creates the event and the snapshot.
//
// # Parameters
// - event is an event to store.
// - aggregate is an aggregate to store.
// # Returns
// - an error
func (es *EventStoreOnDynamoDB) createEventAndSnapshot(ctx context.Context, event Event, aggregate Aggregate) error {
	if event == nil {
		return errors.New("event is nil")
	}
	putJournal, err := es.putJournal(event)
	if err != nil {
		return err
	}
	putSnapshot, err := es.putSnapshot(event, 0, aggregate)
	if err != nil {
		return err
	}

	transactItems := []types.TransactWriteItem{
		{Put: putSnapshot},
		{Put: putJournal},
	}
	if es.keepSnapshot {
		putSnapshot2, err := es.putSnapshot(event, aggregate.GetSeqNr(), aggregate)
		if err != nil {
			return err
		}
		transactItems = append(transactItems, types.TransactWriteItem{Put: putSnapshot2})
	}

	if _, err = es.client.TransactWriteItems(ctx, &dynamodb.TransactWriteItemsInput{TransactItems: transactItems}); err != nil {
		var t *types.TransactionCanceledException
		switch {
		case errors.As(err, &t):
			for _, reason := range t.CancellationReasons {
				if reason.Code != nil && *reason.Code == "ConditionalCheckFailed" {
					return NewOptimisticLockError("Transaction write was canceled due to conditional check failure", err)
				}
			}
			return NewIOError("Failed to transact write items due to non-conditional check failure", err)
		default:
			return NewIOError("Failed to transact write items", err)
		}
	}

	return nil
}

// deleteExcessSnapshots deletes excess snapshots.
//
// # Parameters
// - aggregateId is an aggregateId to delete.
// # Returns
// - an error
func (es *EventStoreOnDynamoDB) deleteExcessSnapshots(ctx context.Context, aggregateId AggregateId) error {
	if aggregateId == nil {
		return errors.New("aggregateId is nil")
	}
	if es.keepSnapshot && es.keepSnapshotCount > 0 {
		snapshotCount, err := es.getSnapshotCount(ctx, aggregateId)
		if err != nil {
			return err
		}
		snapshotCount -= 1
		excessCount := uint32(snapshotCount) - es.keepSnapshotCount
		if excessCount > 0 {
			keys, err := es.getLastSnapshotKeys(ctx, aggregateId, int32(excessCount))
			if err != nil {
				return err
			}
			var requests []types.WriteRequest
			for _, key := range keys {
				request := types.WriteRequest{
					DeleteRequest: &types.DeleteRequest{
						Key: map[string]types.AttributeValue{
							"pkey": &types.AttributeValueMemberS{Value: key.pkey},
							"skey": &types.AttributeValueMemberS{Value: key.skey},
						},
					},
				}
				requests = append(requests, request)
			}
			request := &dynamodb.BatchWriteItemInput{RequestItems: map[string][]types.WriteRequest{es.snapshotTableName: requests}}
			if _, err = es.client.BatchWriteItem(context.Background(), request); err != nil {
				return NewIOError("Failed to deleteExcessSnapshots updateItem", err)
			}
		}
	}

	return nil
}

// updateTtlOfExcessSnapshots updates the ttl of excess snapshots.
//
// # Parameters
// - aggregateId is an aggregateId to update.
// # Returns
// - an error
func (es *EventStoreOnDynamoDB) updateTtlOfExcessSnapshots(ctx context.Context, aggregateId AggregateId) error {
	if aggregateId == nil {
		return errors.New("aggregateId is nil")
	}
	if es.keepSnapshot && es.keepSnapshotCount > 0 {
		snapshotCount, err := es.getSnapshotCount(ctx, aggregateId)
		if err != nil {
			return err
		}
		snapshotCount -= 1
		excessCount := uint32(snapshotCount) - es.keepSnapshotCount
		if excessCount > 0 {
			keys, err := es.getLastSnapshotKeys(ctx, aggregateId, int32(excessCount))
			if err != nil {
				return err
			}
			ttl := time.Now().Add(es.deleteTtl).Unix()
			for _, key := range keys {
				request := &dynamodb.UpdateItemInput{
					TableName: aws.String(es.snapshotTableName),
					Key: map[string]types.AttributeValue{
						"pkey": &types.AttributeValueMemberS{Value: key.pkey},
						"skey": &types.AttributeValueMemberS{Value: key.skey},
					},
					UpdateExpression: aws.String("SET #ttl=:ttl"),
					ExpressionAttributeNames: map[string]string{
						"#ttl": "ttl",
					},
					ExpressionAttributeValues: map[string]types.AttributeValue{
						":ttl": &types.AttributeValueMemberN{Value: strconv.FormatInt(ttl, 10)},
					},
				}
				if _, err := es.client.UpdateItem(ctx, request); err != nil {
					return NewIOError("Failed to updateTtlOfExcessSnapshots updateItem", err)
				}
			}
		}
	}

	return nil
}

// getSnapshotCount returns a snapshot count.
//
// # Parameters
// - aggregateId is an aggregateId to get.
// # Returns
// - a snapshot count
// - an error
func (es *EventStoreOnDynamoDB) getSnapshotCount(ctx context.Context, aggregateId AggregateId) (int32, error) {
	if aggregateId == nil {
		return 0, errors.New("aggregateId is nil")
	}

	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid"),
		ExpressionAttributeNames: map[string]string{
			"#aid": "aid",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid": &types.AttributeValueMemberS{Value: aggregateId.AsString()},
		},
		Select: types.SelectCount,
	}
	response, err := es.client.Query(ctx, request)
	if err != nil {
		return 0, NewIOError("Failed to getSnapshotCount query", err)
	}
	return response.Count, nil
}

type pkeyAndSkey struct {
	pkey string
	skey string
}

// getLastSnapshotKeys returns the last snapshot keys.
//
// # Parameters
// - aggregateId is an aggregateId to get.
// - limit is a limit of the number of keys to get.
// # Returns
// - a list of keys
// - an error
func (es *EventStoreOnDynamoDB) getLastSnapshotKeys(ctx context.Context, aggregateId AggregateId, limit int32) ([]pkeyAndSkey, error) {
	if aggregateId == nil {
		return nil, errors.New("aggregateId is nil")
	}

	request := &dynamodb.QueryInput{
		TableName:              aws.String(es.snapshotTableName),
		IndexName:              aws.String(es.snapshotAidIndexName),
		KeyConditionExpression: aws.String("#aid = :aid AND #seq_nr > :seq_nr"),
		ExpressionAttributeNames: map[string]string{
			"#aid":    "aid",
			"#seq_nr": "seq_nr",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":aid":    &types.AttributeValueMemberS{Value: aggregateId.AsString()},
			":seq_nr": &types.AttributeValueMemberN{Value: "0"},
		},
		ScanIndexForward: aws.Bool(false),
		Limit:            aws.Int32(limit),
	}
	if es.deleteTtl < math.MaxInt64 {
		request.FilterExpression = aws.String("#ttl = :ttl")
		request.ExpressionAttributeNames["#ttl"] = "ttl"
		request.ExpressionAttributeValues[":ttl"] = &types.AttributeValueMemberN{Value: "0"}
	}
	response, err := es.client.Query(ctx, request)
	if err != nil {
		return nil, NewIOError("Failed to getLastSnapshotKeys query", err)
	}

	var pkeySkeys []pkeyAndSkey
	for _, item := range response.Items {
		pkey := item["pkey"].(*types.AttributeValueMemberS).Value
		skey := item["skey"].(*types.AttributeValueMemberS).Value
		pkeySkey := pkeyAndSkey{
			pkey: pkey,
			skey: skey,
		}
		pkeySkeys = append(pkeySkeys, pkeySkey)
	}

	return pkeySkeys, nil
}
