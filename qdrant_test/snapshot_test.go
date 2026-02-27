package qdrant_test

import (
	"context"
	"testing"

	"github.com/al-masood/qdrant-http-go-client/qdrant"
)

func TestCreateCollectionSnapshot(t *testing.T) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   "localhost",
		Port:   6333,
		APIKey: "v5TWCCV59zfRr8Vg",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	resp, err := client.CreateCollectionSnapshot(ctx, "test_collection")
	if err != nil {
		t.Fatalf("failed to create collection snapshot: %v", err)
	}

	if resp == nil {
		t.Fatal("response is nil")
	}

	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}

	t.Logf("Snapshot created: %+v", resp.Result)
}

func TestGetClusterInfo(t *testing.T) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   "localhost",
		Port:   6333,
		APIKey: "v5TWCCV59zfRr8Vg",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	resp, err := client.GetClusterInfo(ctx)
	if err != nil {
		t.Fatalf("failed to get cluster info: %v", err)
	}

	if resp == nil {
		t.Fatal("response is nil")
	}

	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}

	if resp.Result.PeerID == 0 {
		t.Error("expected peer_id to be set")
	}

	if resp.Result.Peers == nil {
		t.Error("expected peers to be not nil")
	}

	t.Logf("Cluster Info: PeerID=%d, Peers=%v", resp.Result.PeerID, resp.Result.Peers)
}

func TestListCollections(t *testing.T) {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   "localhost",
		Port:   6333,
		APIKey: "v5TWCCV59zfRr8Vg",
	})
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	ctx := context.Background()
	resp, err := client.ListCollections(ctx)
	if err != nil {
		t.Fatalf("failed to list collections: %v", err)
	}

	if resp == nil {
		t.Fatal("response is nil")
	}

	if resp.Status != "ok" {
		t.Errorf("expected status ok, got %s", resp.Status)
	}

	if resp.Result == nil {
		t.Fatal("expected result to be not nil")
	}

	if resp.Result.Collections == nil {
		t.Error("expected collections to be not nil")
	}

	t.Logf("Found %d collections", len(resp.Result.Collections))
	for _, col := range resp.Result.Collections {
		t.Logf("Collection: %s", col.Name)
	}
}
