package main

import (
	"log"
	"net/http"
	"urls_etl/internal/infra/mock_server"
)

func main() {
	http.HandleFunc("/getItems", mock_server.GetItemsHandler)

	http.HandleFunc("/service1/fillItems", mock_server.FillItemsHandler)
	http.HandleFunc("/service2/scoreItems", mock_server.ScoreItemsHandler)
	http.HandleFunc("/service3/logItems", mock_server.LogItemsHandler)

	log.Println("mock server started on :8080")

	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
