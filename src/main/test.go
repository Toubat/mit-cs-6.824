package main

import (
	"encoding/json"
	"log"
	"os"
)

type KeyValue struct {
	Key   string
	Value string
}

func main() {
	kva := []KeyValue{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
	}

	// craete "test.json"
	file, err := os.Create("test.txt")
	if err != nil {
		log.Fatal(err)
	}

	enc := json.NewEncoder(file)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}

}
