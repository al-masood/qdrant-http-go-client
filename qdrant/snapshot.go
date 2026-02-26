package qdrant

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Snapshot represents a shard snapshot metadata
type Snapshot struct {
	Name         string `json:"name"`
	Size         uint64 `json:"size"`
	CreationTime string `json:"creation_time"`
	Checksum     string `json:"checksum"`
}

// SnapshotDescription represents detailed snapshot information
type SnapshotDescription struct {
	Name                     string `json:"name"`
	CreationTime             string `json:"creation_time"`
	Size                     uint64 `json:"size"`
	Checksum                 string `json:"checksum"`
	CollectionName           string `json:"collection_name"`
	CollectionSnapshotCount  int    `json:"collection_snapshot_count"`
	PayloadIndexSnapshotSize uint64 `json:"payload_index_snapshot_size"`
	VectorDataSnapshotSize   uint64 `json:"vector_data_snapshot_size"`
}

// ListFullSnapshotsResponse represents the response from listing full storage snapshots
type ListFullSnapshotsResponse struct {
	Time   float64               `json:"time"`
	Status string                `json:"status"`
	Result []SnapshotDescription `json:"result"`
}

// CreateFullSnapshotResponse represents the response from creating a full storage snapshot
type CreateFullSnapshotResponse struct {
	Time   float64             `json:"time"`
	Status string              `json:"status"`
	Result SnapshotDescription `json:"result"`
}

// DeleteFullSnapshotResponse represents the response from deleting a full storage snapshot
type DeleteFullSnapshotResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// RestoreFullSnapshotResponse represents the response from restoring a full storage snapshot
type RestoreFullSnapshotResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// ListCollectionSnapshotsResponse represents the response from listing collection snapshots
type ListCollectionSnapshotsResponse struct {
	Time   float64    `json:"time"`
	Status string     `json:"status"`
	Result []Snapshot `json:"result"`
}

// CreateCollectionSnapshotResponse represents the response from creating a collection snapshot
type CreateCollectionSnapshotResponse struct {
	Time   float64  `json:"time"`
	Status string   `json:"status"`
	Result Snapshot `json:"result"`
}

// DeleteCollectionSnapshotResponse represents the response from deleting a collection snapshot
type DeleteCollectionSnapshotResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// RestoreCollectionSnapshotResponse represents the response from restoring a collection snapshot
type RestoreCollectionSnapshotResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// CreateFullSnapshot creates a new full storage snapshot
func (c *Client) CreateFullSnapshot(ctx context.Context) (*CreateFullSnapshotResponse, error) {
	path := "/snapshots"

	req, err := c.NewRequest(ctx, http.MethodPost, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response CreateFullSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// ListFullSnapshots lists all full storage snapshots
func (c *Client) ListFullSnapshots(ctx context.Context) (*ListFullSnapshotsResponse, error) {
	path := "/snapshots"

	req, err := c.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response ListFullSnapshotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// DeleteFullSnapshot deletes a specific full storage snapshot
func (c *Client) DeleteFullSnapshot(ctx context.Context, snapshotName string) (*DeleteFullSnapshotResponse, error) {
	path := fmt.Sprintf("/snapshots/%s", snapshotName)

	req, err := c.NewRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response DeleteFullSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// RestoreFullSnapshot restores a full storage snapshot from a specified location
func (c *Client) RestoreFullSnapshot(ctx context.Context, location string) (*RestoreFullSnapshotResponse, error) {
	path := "/snapshots/upload"

	requestBody := map[string]string{
		"location": location,
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	req, err := c.NewRequest(ctx, http.MethodPut, path, toReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response RestoreFullSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// CreateCollectionSnapshot creates a new snapshot for a collection
func (c *Client) CreateCollectionSnapshot(ctx context.Context, collectionName string) (*CreateCollectionSnapshotResponse, error) {
	path := fmt.Sprintf("/collections/%s/snapshots", collectionName)

	req, err := c.NewRequest(ctx, http.MethodPost, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response CreateCollectionSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// ListCollectionSnapshots lists all snapshots for a collection
func (c *Client) ListCollectionSnapshots(ctx context.Context, collectionName string) (*ListCollectionSnapshotsResponse, error) {
	path := fmt.Sprintf("/collections/%s/snapshots", collectionName)

	req, err := c.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response ListCollectionSnapshotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// DeleteCollectionSnapshot deletes a specific snapshot for a collection
func (c *Client) DeleteCollectionSnapshot(ctx context.Context, collectionName string, snapshotName string) (*DeleteCollectionSnapshotResponse, error) {
	path := fmt.Sprintf("/collections/%s/snapshots/%s", collectionName, snapshotName)

	req, err := c.NewRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response DeleteCollectionSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// RestoreCollectionSnapshot restores a collection snapshot from a specified location
func (c *Client) RestoreCollectionSnapshot(ctx context.Context, collectionName string, location string) (*RestoreCollectionSnapshotResponse, error) {
	path := fmt.Sprintf("/collections/%s/snapshots/upload", collectionName)

	requestBody := map[string]string{
		"location": location,
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	req, err := c.NewRequest(ctx, http.MethodPut, path, toReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response RestoreCollectionSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// DownloadCollectionSnapshot downloads a collection snapshot file
// Returns the response body containing the snapshot file data
// The caller is responsible for closing the returned reader
func (c *Client) DownloadCollectionSnapshot(ctx context.Context, collectionName string, snapshotName string) (io.ReadCloser, error) {
	path := fmt.Sprintf("/collections/%s/snapshots/%s", collectionName, snapshotName)

	req, err := c.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return resp.Body, nil
}

// CreateSnapshotResponse represents the response from creating a snapshot
type CreateSnapshotResponse struct {
	Time   float64  `json:"time"`
	Status string   `json:"status"`
	Result Snapshot `json:"result"`
}

// ListShardSnapshotsResponse represents the response from listing shard snapshots
type ListShardSnapshotsResponse struct {
	Time   float64    `json:"time"`
	Status string     `json:"status"`
	Result []Snapshot `json:"result"`
}

// DeleteShardSnapshotResponse represents the response from deleting a shard snapshot
type DeleteShardSnapshotResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// RestoreShardSnapshotResponse represents the response from restoring a shard snapshot
type RestoreShardSnapshotResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// CreateShardSnapshot creates a new snapshot for a specific shard
func (c *Client) CreateShardSnapshot(ctx context.Context, collectionName string, shardID string) (*CreateSnapshotResponse, error) {
	path := fmt.Sprintf("/collections/%s/shards/%s/snapshots", collectionName, shardID)

	req, err := c.NewRequest(ctx, http.MethodPost, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response CreateSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// ListShardSnapshots lists all snapshots for a specific shard
func (c *Client) ListShardSnapshots(ctx context.Context, collectionName string, shardID string) (*ListShardSnapshotsResponse, error) {
	path := fmt.Sprintf("/collections/%s/shards/%s/snapshots", collectionName, shardID)

	req, err := c.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response ListShardSnapshotsResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// DeleteShardSnapshot deletes a specific snapshot for a shard
func (c *Client) DeleteShardSnapshot(ctx context.Context, collectionName string, shardID string, snapshotName string) (*DeleteShardSnapshotResponse, error) {
	path := fmt.Sprintf("/collections/%s/shards/%s/snapshots/%s", collectionName, shardID, snapshotName)

	req, err := c.NewRequest(ctx, http.MethodDelete, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response DeleteShardSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// RestoreShardSnapshot restores a shard snapshot from a specified location
// The location can be a URL or a file path depending on the Qdrant configuration
func (c *Client) RestoreShardSnapshot(ctx context.Context, collectionName string, shardID string, snapshotName string, location string) (*RestoreShardSnapshotResponse, error) {
	path := fmt.Sprintf("/collections/%s/shards/%s/snapshots/upload", collectionName, shardID)

	requestBody := map[string]string{
		"location": location,
	}

	bodyBytes, err := json.Marshal(requestBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	req, err := c.NewRequest(ctx, http.MethodPut, path, toReader(bodyBytes))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var response RestoreShardSnapshotResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// DownloadShardSnapshot downloads a shard snapshot file
// Returns the response body containing the snapshot file data
// The caller is responsible for closing the returned reader
func (c *Client) DownloadShardSnapshot(ctx context.Context, collectionName string, shardID int, snapshotName string) (io.ReadCloser, error) {
	path := fmt.Sprintf("/collections/%s/shards/%d/snapshots/%s", collectionName, shardID, snapshotName)

	req, err := c.NewRequest(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	resp, err := c.Do(req)
	if err != nil {
		return nil, fmt.Errorf("executing request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(bodyBytes))
	}

	return resp.Body, nil
}

// toReader converts a byte slice to an io.Reader
func toReader(b []byte) io.Reader {
	return &byteReader{data: b}
}

// byteReader implements io.Reader for a byte slice
type byteReader struct {
	data []byte
	pos  int
}

func (r *byteReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
