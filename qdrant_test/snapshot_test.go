package qdrant_test

import (
	"testing"

	"github.com/al-masood/qdrant-http-go-client/qdrant"
)

func TestSnapshot(t *testing.T) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   "localhost",
		Port:   6333,
		APIKey: "",
	})
}
