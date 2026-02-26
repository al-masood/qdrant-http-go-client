package main

import (
	"context"
	"fmt"

	"github.com/al-masood/qdrant-http-go-client/qdrant"
)

func main() {
	client, err := qdrant.NewClient(&qdrant.Config{
		Host:   "localhost",
		Port:   6333,
		APIKey: "",
	})
	if err != nil {
		fmt.Printf("can't connect: %v\n", err)
		return
	}
	ctx := context.Background()

	// Test ListCollections
	fmt.Println("=== ListCollections ===")
	listResp, err := client.ListCollections(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("%+v\n", listResp)
	}

	// Test GetClusterInfo
	fmt.Println("\n=== GetClusterInfo ===")
	clusterResp, err := client.GetClusterInfo(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
	} else {
		fmt.Printf("%+v\n", clusterResp)
	}
}
