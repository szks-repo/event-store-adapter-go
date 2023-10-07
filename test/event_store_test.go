package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	event_store_adapter_go "github.com/j5ik2o/event-store-adapter-go"
	"github.com/j5ik2o/event-store-adapter-go/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/localstack"
)

func Test_WriteAndRead(t *testing.T) {
	// Given
	ctx := context.Background()

	container, err := localstack.RunContainer(
		ctx,
		testcontainers.CustomizeRequest(testcontainers.GenericContainerRequest{
			ContainerRequest: testcontainers.ContainerRequest{
				Image: "localstack/localstack:2.1.0",
				Env: map[string]string{
					"SERVICES":              "dynamodb",
					"DEFAULT_REGION":        "us-east-1",
					"EAGER_SERVICE_LOADING": "1",
					"DYNAMODB_SHARED_DB":    "1",
					"DYNAMODB_IN_MEMORY":    "1",
				},
			},
		}),
	)
	assert.NotNil(t, container)
	require.Nil(t, err)

	dynamodbClient, err := common.CreateDynamoDBClient(t, ctx, container)
	assert.NotNil(t, dynamodbClient)
	require.Nil(t, err)

	err = common.CreateJournalTable(t, ctx, dynamodbClient, "journal", "journal-aid-index")
	require.Nil(t, err)

	err = common.CreateSnapshotTable(t, ctx, dynamodbClient, "snapshot", "snapshot-aid-index")
	require.Nil(t, err)

	// When
	snapshotConverter := func(m map[string]interface{}) (event_store_adapter_go.Aggregate, error) {
		idMap, ok := m["Id"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("Id is not a map")
		}
		value, ok := idMap["Value"].(string)
		if !ok {
			return nil, fmt.Errorf("Value is not a float64")
		}
		userAccountId := newUserAccountId(value)
		result, _ := newUserAccount(userAccountId, m["Name"].(string))
		return result, nil
	}

	eventConverter := func(m map[string]interface{}) (event_store_adapter_go.Event, error) {
		aggregateMap, ok := m["AggregateId"].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("AggregateId is not a map")
		}
		aggregateId, ok := aggregateMap["Value"].(string)
		if !ok {
			return nil, fmt.Errorf("Value is not a float64")
		}
		switch m["TypeName"].(string) {
		case "UserAccountCreated":
			userAccountId := newUserAccountId(aggregateId)
			return newUserAccountCreated(
				m["Id"].(string),
				&userAccountId,
				uint64(m["SeqNr"].(float64)),
				m["Name"].(string),
				uint64(m["OccurredAt"].(float64)),
			), nil
		case "UserAccountNameChanged":
			userAccountId := newUserAccountId(aggregateId)
			return newUserAccountNameChanged(
				m["Id"].(string),
				&userAccountId,
				uint64(m["SeqNr"].(float64)),
				m["Name"].(string),
				uint64(m["OccurredAt"].(float64)),
			), nil
		default:
			return nil, fmt.Errorf("unknown event type")
		}
	}

	eventStore, err := event_store_adapter_go.NewEventStore(
		dynamodbClient,
		"journal",
		"snapshot",
		"journal-aid-index",
		"snapshot-aid-index",
		1,
		eventConverter,
		snapshotConverter,
		event_store_adapter_go.WithKeepSnapshot(true),
		event_store_adapter_go.WithDeleteTtl(1*time.Second))
	require.NotNil(t, eventStore)
	require.Nil(t, err)

	userAccountId1 := newUserAccountId("1")

	initial, userAccountCreated := newUserAccount(userAccountId1, "test")
	require.NotNil(t, initial)
	require.NotNil(t, userAccountCreated)
	t.Logf("initial: %v", initial)
	t.Logf("userAccountCreated: %v", userAccountCreated)

	err = eventStore.PersistEventAndSnapshot(
		userAccountCreated,
		initial,
	)
	require.Nil(t, err)

	updated, err := initial.Rename("test2")
	require.NotNil(t, updated)
	require.Nil(t, err)
	t.Logf("updated: %v", updated)

	err = eventStore.PersistEvent(
		updated.Event,
		updated.Aggregate.Version,
	)
	require.Nil(t, err)

	snapshotResult, err := eventStore.GetLatestSnapshotById(&userAccountId1)
	require.NotNil(t, snapshotResult)
	require.Nil(t, err)
	t.Logf("snapshotResult: %v", snapshotResult)

	userAccount1, ok := snapshotResult.Aggregate().(*userAccount)
	require.NotNil(t, userAccount1)
	require.NotNil(t, ok)
	t.Logf("UserAccount: %v", userAccount1)

	events, err := eventStore.GetEventsByIdSinceSeqNr(&userAccountId1, userAccount1.GetSeqNr()+1)
	require.NotNil(t, events)
	require.Nil(t, err)
	t.Logf("Events: %v", events)

	actual := replayUserAccount(events, snapshotResult.Aggregate().(*userAccount))
	require.NotNil(t, actual)
	t.Logf("Actual: %v", actual)

	expect, _ := newUserAccount(userAccountId1, "test2")
	require.NotNil(t, expect)

	// Then
	assert.True(t, actual.Equals(expect))

	defer func() {
		if err := container.Terminate(ctx); err != nil {
			t.Fatalf("failed to terminate container: %s", err.Error())
		}
	}()
}