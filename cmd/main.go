package main

import (
	"log"

	"github.com/szks-repo/event-store-adapter-go/pkg"
	"github.com/szks-repo/event-store-adapter-go/web"
)

func main() {
	// make http server
	srv := web.NewServer(
		pkg.NewUserAccountRepository(pkg.NewEventStoreOnMemory()),
	)

	log.Printf("Server is running on %s\n", srv.Addr)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatal(err)
	}
}
