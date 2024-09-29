package main

import (
	"encoding/json"
	"go_bitcask"
	"net/http"
)

var db *go_bitcask.DB

func init() {

}

func handlerPut(w http.ResponseWriter, r *http.Request) {
	var data map[string]string

	if err := json.NewDecoder(r.Body).Decode(data); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for key, value := range data {
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
	}

}

func main() {
	http.HandleFunc("/go_bitcask/put", handlerPut)
	http.ListenAndServe("localhost:8080", nil)
}
