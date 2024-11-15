package test

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	esag "github.com/szks-repo/event-store-adapter-go/pkg"

	"github.com/oklog/ulid/v2"
)

type userAccountId struct {
	Value string
}

func newUserAccountId(value string) userAccountId {
	return userAccountId{Value: value}
}

func (id *userAccountId) GetTypeName() string {
	return "UserAccountId"
}

func (id *userAccountId) GetValue() string {
	return id.Value
}

func (id *userAccountId) String() string {
	return fmt.Sprintf("userAccount{TypeName: %s, Valuie: %s}", id.GetTypeName(), id.Value)
}

func (id *userAccountId) AsString() string {
	return fmt.Sprintf("%s-%s", id.GetTypeName(), id.Value)
}

type userAccount struct {
	Id      userAccountId
	Name    string
	SeqNr   uint64
	Version uint64
	mu      sync.Mutex
}

func newUserAccount(id userAccountId, name string) (*userAccount, *userAccountCreated) {
	aggregate := userAccount{
		Id:      id,
		Name:    name,
		SeqNr:   0,
		Version: 1,
	}
	aggregate.SeqNr += 1
	eventId := newULID()
	return &aggregate, newUserAccountCreated(eventId.String(), &id, aggregate.SeqNr, name, uint64(time.Now().UnixNano()))
}

func replayUserAccount(events []esag.Event, snapshot *userAccount) *userAccount {
	result := snapshot
	for _, event := range events {
		result = result.applyEvent(event)
	}
	return result
}

func (ua *userAccount) applyEvent(event esag.Event) *userAccount {
	switch e := event.(type) {
	case *userAccountNameChanged:
		update, err := ua.Rename(e.Name)
		if err != nil {
			panic(err)
		}
		return update.Aggregate
	}
	return ua
}

func (ua *userAccount) String() string {
	return fmt.Sprintf("UserAccount{Id: %s, Name: %s}", ua.Id.String(), ua.Name)
}

func (ua *userAccount) GetId() esag.AggregateId {
	return &ua.Id
}

func (ua *userAccount) GetSeqNr() uint64 {
	return ua.SeqNr
}

func (ua *userAccount) GetVersion() uint64 {
	return ua.Version
}

func (ua *userAccount) WithVersion(version uint64) esag.Aggregate {
	result := *ua
	result.Version = version
	return &result
}

type userAccountResult struct {
	Aggregate *userAccount
	Event     *userAccountNameChanged
}

func (ua *userAccount) Rename(name string) (*userAccountResult, error) {
	updatedUserAccount := *ua
	updatedUserAccount.Name = name
	updatedUserAccount.IncrementSeq()
	event := newUserAccountNameChanged(newULID().String(), &ua.Id, updatedUserAccount.SeqNr, name, uint64(time.Now().UnixNano()))
	return &userAccountResult{&updatedUserAccount, event}, nil
}

func (ua *userAccount) Equals(other *userAccount) bool {
	return ua.Id.Value == other.Id.Value && ua.Name == other.Name
}

func (ua *userAccount) IncrementSeq() {
	ua.SeqNr++
}

func newULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
