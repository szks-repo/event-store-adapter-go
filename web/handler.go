package web

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/szks-repo/event-store-adapter-go/pkg"
)

type handler struct {
	userAccountRepository pkg.UserAccountRepository
}

type jsonDto[E pkg.Event, A pkg.Aggregate] struct {
	Event     E `json:"event,omitempty"`
	Aggregate A `json:"aggregate,omitempty"`
}

func (h *handler) CreateUserAccountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("CreateUserAccountHandler")

	userAccountId := pkg.NewUserAccountId(fmt.Sprintf("usr_%d", time.Now().Unix()))
	userAccount, userAccountCreated := pkg.NewUserAccount(userAccountId, "Tom")

	if err := h.userAccountRepository.StoreEventAndSnapshot(r.Context(), userAccountCreated, userAccount); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonPayload, err := json.Marshal(jsonDto[*pkg.UserAccountCreated, *pkg.UserAccount]{
		Event:     userAccountCreated,
		Aggregate: userAccount,
	})
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonPayload)
}

func (h *handler) UpdateUserAccountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("UpdateUserAccountHandler")

	userAccountIdFromQueryString := r.FormValue("userAccountId")

	userAccount, err := h.userAccountRepository.FindById(r.Context(), pkg.NewUserAccountId(userAccountIdFromQueryString))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	result, err := userAccount.Rename("Jerry")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := h.userAccountRepository.StoreEventAndSnapshot(r.Context(), result.Event, result.Aggregate); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonPayload, err := json.Marshal(jsonDto[*pkg.UserAccountNameChanged, *pkg.UserAccount]{
		Event:     result.Event,
		Aggregate: result.Aggregate,
	})
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonPayload)
}

func (h *handler) GetUserAccountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("GetUserAccountHandler")

	userAccountIdFromQueryString := r.FormValue("userAccountId")

	userAccount, err := h.userAccountRepository.FindById(r.Context(), pkg.NewUserAccountId(userAccountIdFromQueryString))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	jsonPayload, err := json.Marshal(jsonDto[*pkg.UserAccountCreated, *pkg.UserAccount]{
		Aggregate: userAccount,
	})
	if err != nil {
		panic(err)
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(jsonPayload)
}

func (h *handler) ListUserAccountHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("ListUserAccountHandler")

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ListUserAccountHandler"))
}

func (h *handler) IndexHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("IndexHandler")

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<html>
<head>
	<title>Event Store Adapter</title>
</head>
<body>
	<h1>Event Store Adapter</h1>
	<ul>
		<li><a href="/userAccounts/create">Create User Account</a></li>
		<li><a href="/userAccounts/update?userAccountId=">Update User Account</a></li>
		<li><a href="/userAccounts/get?userAccountId=">Get User Account</a></li>
	</ul>
</body>	
</html`))
}
