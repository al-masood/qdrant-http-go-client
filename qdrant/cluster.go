package qdrant

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// PeerState represents the state of a peer in the cluster
type PeerState struct {
	PeerID uint64 `json:"peer_id"`
	URI    string `json:"uri"`
	State  string `json:"state"`
}

// ShardInfo represents information about a shard
type ShardInfo struct {
	ShardID     uint64   `json:"shard_id"`
	State       string   `json:"state"`
	ReplicaSets []uint64 `json:"replica_sets"`
}

// LocalShardInfo represents information about a local shard
type LocalShardInfo struct {
	ShardID uint64 `json:"shard_id"`
	State   string `json:"state"`
}

// RemoteShardInfo represents information about a remote shard
type RemoteShardInfo struct {
	ShardID  uint64 `json:"shard_id"`
	State    string `json:"state"`
	PeerID   uint64 `json:"peer_id"`
	PeerURI  string `json:"peer_uri"`
}

// ReplicaSetShard represents a shard in a replica set
type ReplicaSetShard struct {
	PeerID uint64 `json:"peer_id"`
	State  string `json:"state"`
}

// ReplicaSet represents a replica set for a shard
type ReplicaSet struct {
	ShardID       uint64            `json:"shard_id"`
	ReplicaCount  int               `json:"replica_count"`
	Replicas      []ReplicaSetShard `json:"replicas"`
	LocalShard    *LocalShardInfo   `json:"local_shard,omitempty"`
	RemoteShards  []RemoteShardInfo `json:"remote_shards,omitempty"`
}

// CollectionClusterInfo represents cluster information for a collection
type CollectionClusterInfo struct {
	ShardCount       int                    `json:"shard_count"`
	ReplicaCount     int                    `json:"replica_count"`
	PeerID           uint64                 `json:"peer_id"`
	Peers            map[string]PeerState   `json:"peers"`
	LocalShards      []LocalShardInfo        `json:"local_shards"`
	RemoteShards     []RemoteShardInfo       `json:"remote_shards"`
	ShardTransfers   []ShardTransfer         `json:"shard_transfers"`
}

// ShardTransfer represents information about a shard transfer
type ShardTransfer struct {
	ShardID       uint64 `json:"shard_id"`
	FromPeerID    uint64 `json:"from_peer_id"`
	ToPeerID      uint64 `json:"to_peer_id"`
	FromPeerURI   string `json:"from_peer_uri,omitempty"`
	ToPeerURI     string `json:"to_peer_uri,omitempty"`
	SyncState     string `json:"sync_state,omitempty"`
}

// ClusterInfo represents the overall cluster information
type ClusterInfo struct {
	PeerID                  uint64                   `json:"peer_id"`
	Peers                   map[string]PeerState     `json:"peers"`
	ShardTransfers          []ShardTransfer          `json:"shard_transfers"`
	ConsensusThreadStatus   map[string]interface{}  `json:"consensus_thread_status"`
	MessageSendFailures     map[string]int          `json:"message_send_failures"`
}

// GetClusterInfoResponse represents the response from getting cluster info
type GetClusterInfoResponse struct {
	Time   float64     `json:"time"`
	Status string      `json:"status"`
	Result ClusterInfo `json:"result"`
}

// GetCollectionClusterInfoResponse represents the response from getting collection cluster info
type GetCollectionClusterInfoResponse struct {
	Time   float64               `json:"time"`
	Status string                `json:"status"`
	Result CollectionClusterInfo `json:"result"`
}

// UpdateCollectionClusterSetupRequest represents the request body for updating cluster setup
type UpdateCollectionClusterSetupRequest struct {
	MoveShard       *MoveShardRequest       `json:"move_shard,omitempty"`
	ReplicateShard  *ReplicateShardRequest  `json:"replicate_shard,omitempty"`
	DropReplica     *DropReplicaRequest     `json:"drop_replica,omitempty"`
	AbortTransfer   *AbortTransferRequest   `json:"abort_transfer,omitempty"`
	RestartTransfer *RestartTransferRequest `json:"restart_transfer,omitempty"`
}

// MoveShardRequest represents a request to move a shard
type MoveShardRequest struct {
	ShardID    uint64 `json:"shard_id"`
	FromPeerID uint64 `json:"from_peer_id"`
	ToPeerID   uint64 `json:"to_peer_id"`
}

// ReplicateShardRequest represents a request to replicate a shard
type ReplicateShardRequest struct {
	ShardID  uint64 `json:"shard_id"`
	PeerID   uint64 `json:"peer_id"`
}

// DropReplicaRequest represents a request to drop a replica
type DropReplicaRequest struct {
	ShardID uint64 `json:"shard_id"`
	PeerID  uint64 `json:"peer_id"`
}

// AbortTransferRequest represents a request to abort a shard transfer
type AbortTransferRequest struct {
	ShardID    uint64 `json:"shard_id"`
	FromPeerID uint64 `json:"from_peer_id"`
	ToPeerID   uint64 `json:"to_peer_id"`
}

// RestartTransferRequest represents a request to restart a shard transfer
type RestartTransferRequest struct {
	ShardID    uint64 `json:"shard_id"`
	FromPeerID uint64 `json:"from_peer_id"`
	ToPeerID   uint64 `json:"to_peer_id"`
}

// UpdateCollectionClusterSetupResponse represents the response from updating cluster setup
type UpdateCollectionClusterSetupResponse struct {
	Time   float64 `json:"time"`
	Status string  `json:"status"`
	Result bool    `json:"result"`
}

// GetClusterInfo retrieves information about the cluster
func (c *Client) GetClusterInfo(ctx context.Context) (*GetClusterInfoResponse, error) {
	path := "/cluster"

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

	var response GetClusterInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// GetCollectionClusterInfo retrieves cluster information for a specific collection
func (c *Client) GetCollectionClusterInfo(ctx context.Context, collectionName string) (*GetCollectionClusterInfoResponse, error) {
	path := fmt.Sprintf("/collections/%s/cluster", collectionName)

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

	var response GetCollectionClusterInfoResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// UpdateCollectionClusterSetup updates the cluster setup for a collection
func (c *Client) UpdateCollectionClusterSetup(ctx context.Context, collectionName string, request *UpdateCollectionClusterSetupRequest) (*UpdateCollectionClusterSetupResponse, error) {
	path := fmt.Sprintf("/collections/%s/cluster", collectionName)

	bodyBytes, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("marshaling request body: %w", err)
	}

	req, err := c.NewRequest(ctx, http.MethodPost, path, toReader(bodyBytes))
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

	var response UpdateCollectionClusterSetupResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("decoding response: %w", err)
	}

	return &response, nil
}

// MoveShard moves a shard from one peer to another
func (c *Client) MoveShard(ctx context.Context, collectionName string, shardID uint64, fromPeerID uint64, toPeerID uint64) (*UpdateCollectionClusterSetupResponse, error) {
	request := &UpdateCollectionClusterSetupRequest{
		MoveShard: &MoveShardRequest{
			ShardID:    shardID,
			FromPeerID: fromPeerID,
			ToPeerID:   toPeerID,
		},
	}
	return c.UpdateCollectionClusterSetup(ctx, collectionName, request)
}

// ReplicateShard creates a replica of a shard on a specified peer
func (c *Client) ReplicateShard(ctx context.Context, collectionName string, shardID uint64, peerID uint64) (*UpdateCollectionClusterSetupResponse, error) {
	request := &UpdateCollectionClusterSetupRequest{
		ReplicateShard: &ReplicateShardRequest{
			ShardID: shardID,
			PeerID:  peerID,
		},
	}
	return c.UpdateCollectionClusterSetup(ctx, collectionName, request)
}

// DropReplica drops a replica from a specified peer
func (c *Client) DropReplica(ctx context.Context, collectionName string, shardID uint64, peerID uint64) (*UpdateCollectionClusterSetupResponse, error) {
	request := &UpdateCollectionClusterSetupRequest{
		DropReplica: &DropReplicaRequest{
			ShardID: shardID,
			PeerID:  peerID,
		},
	}
	return c.UpdateCollectionClusterSetup(ctx, collectionName, request)
}

// AbortTransfer aborts an ongoing shard transfer
func (c *Client) AbortTransfer(ctx context.Context, collectionName string, shardID uint64, fromPeerID uint64, toPeerID uint64) (*UpdateCollectionClusterSetupResponse, error) {
	request := &UpdateCollectionClusterSetupRequest{
		AbortTransfer: &AbortTransferRequest{
			ShardID:    shardID,
			FromPeerID: fromPeerID,
			ToPeerID:   toPeerID,
		},
	}
	return c.UpdateCollectionClusterSetup(ctx, collectionName, request)
}

// RestartTransfer restarts a shard transfer
func (c *Client) RestartTransfer(ctx context.Context, collectionName string, shardID uint64, fromPeerID uint64, toPeerID uint64) (*UpdateCollectionClusterSetupResponse, error) {
	request := &UpdateCollectionClusterSetupRequest{
		RestartTransfer: &RestartTransferRequest{
			ShardID:    shardID,
			FromPeerID: fromPeerID,
			ToPeerID:   toPeerID,
		},
	}
	return c.UpdateCollectionClusterSetup(ctx, collectionName, request)
}