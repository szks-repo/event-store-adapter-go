package pkg

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

type UserAccountId struct {
	Value string
}

func newUserAccountId(value string) UserAccountId {
	return UserAccountId{Value: value}
}

func NewUserAccountId(value string) UserAccountId {
	return UserAccountId{Value: value}
}

func (id UserAccountId) GetTypeName() string {
	return "UserAccountId"
}

func (id UserAccountId) GetValue() string {
	return id.Value
}

func (id UserAccountId) String() string {
	return fmt.Sprintf("userAccount{TypeName: %s, Valuie: %s}", id.GetTypeName(), id.Value)
}

func (id UserAccountId) AsString() string {
	return fmt.Sprintf("%s-%s", id.GetTypeName(), id.Value)
}

type UserAccount struct {
	Id      UserAccountId
	Name    string
	SeqNr   uint64
	Version uint64
	mu      sync.Mutex
}

func NewUserAccount(accountId UserAccountId, name string) (*UserAccount, *UserAccountCreated) {
	aggregate := &UserAccount{
		Id:      accountId,
		Name:    name,
		SeqNr:   0,
		Version: 1,
	}
	aggregate.IncrementSeq()

	event := NewUserAccountCreated(
		newULID().String(),
		&accountId,
		aggregate.SeqNr,
		name,
		uint64(time.Now().UnixNano()),
	)
	return aggregate, event
}

func replayUserAccount(events []Event, snapshot *UserAccount) *UserAccount {
	result := snapshot
	for _, event := range events {
		result = result.applyEvent(event)
	}
	return result
}

func (ua *UserAccount) applyEvent(event Event) *UserAccount {
	switch e := event.(type) {
	case *UserAccountNameChanged:
		update, err := ua.Rename(e.Name)
		if err != nil {
			panic(err)
		}
		return update.Aggregate
	}
	return ua
}

func (ua *UserAccount) String() string {
	return fmt.Sprintf("UserAccount{Id: %s, Name: %s}", ua.Id.String(), ua.Name)
}

func (ua *UserAccount) GetId() AggregateId {
	return &ua.Id
}

func (ua *UserAccount) GetSeqNr() uint64 {
	return ua.SeqNr
}

func (ua *UserAccount) GetVersion() uint64 {
	return ua.Version
}

func (ua *UserAccount) WithVersion(version uint64) Aggregate {
	result := *ua
	result.Version = version
	return &result
}

type UserAccountResult struct {
	Aggregate *UserAccount
	Event     *UserAccountNameChanged
}

func (ua *UserAccount) Rename(name string) (*UserAccountResult, error) {
	userAccount := *ua
	userAccount.Name = name
	userAccount.IncrementSeq()
	return &UserAccountResult{
		Aggregate: &userAccount,
		Event: NewUserAccountNameChanged(
			newULID().String(),
			&ua.Id,
			userAccount.SeqNr,
			name,
			uint64(time.Now().UnixNano()),
		),
	}, nil
}

func (ua *UserAccount) Equals(other *UserAccount) bool {
	return ua.Id.Value == other.Id.Value && ua.Name == other.Name
}

func (ua *UserAccount) IncrementSeq() {
	ua.SeqNr++
}

func newULID() ulid.ULID {
	t := time.Now()
	entropy := ulid.Monotonic(rand.New(rand.NewSource(t.UnixNano())), 0)
	return ulid.MustNew(ulid.Timestamp(t), entropy)
}
