package web

import (
	"net/http"

	"github.com/szks-repo/event-store-adapter-go/pkg"
)

func NewServer(
	userAccountRepository pkg.UserAccountRepository,
) http.Server {
	handler := &handler{
		userAccountRepository: userAccountRepository,
	}

	http.HandleFunc("/", handler.IndexHandler)
	http.HandleFunc("/userAccounts/get", handler.GetUserAccountHandler)
	http.HandleFunc("/userAccounts/create", handler.CreateUserAccountHandler)
	http.HandleFunc("/userAccounts/update", handler.UpdateUserAccountHandler)

	srv := http.Server{
		Addr:    ":3000",
		Handler: nil,
	}

	return srv
}
