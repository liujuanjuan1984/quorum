package snowman

import (
	"encoding/json"
	"time"

	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type MessageType string

const (
	MessagePutBlock            MessageType = "put_block"
	MessageGetBlock            MessageType = "get_block"
	MessageGetAncestors        MessageType = "get_ancestors"
	MessageAncestors           MessageType = "ancestors"
	MessagePushQuery           MessageType = "push_query"
	MessagePullQuery           MessageType = "pull_query"
	MessageChits               MessageType = "chits"
	MessageGetAcceptedFrontier MessageType = "get_accepted_frontier"
	MessageAcceptedFrontier    MessageType = "accepted_frontier"
	MessageGetAccepted         MessageType = "get_accepted"
	MessageAccepted            MessageType = "accepted"
)

type WireMessage struct {
	GroupID      string            `json:"group_id"`
	Type         MessageType       `json:"type"`
	RequestID    string            `json:"request_id"`
	SenderPubkey string            `json:"sender_pubkey"`
	Timestamp    int64             `json:"timestamp"`
	BlockID      string            `json:"block_id,omitempty"`
	PreferenceID string            `json:"preference_id,omitempty"`
	Block        *quorumpb.Block   `json:"block,omitempty"`
	Blocks       []*quorumpb.Block `json:"blocks,omitempty"`
	BlockIDs     []string          `json:"block_ids,omitempty"`
	Signature    []byte            `json:"signature,omitempty"`
}

func NewWireMessage(groupID string, typ MessageType, requestID string, senderPubkey string) WireMessage {
	return WireMessage{
		GroupID:      groupID,
		Type:         typ,
		RequestID:    requestID,
		SenderPubkey: senderPubkey,
		Timestamp:    time.Now().UnixNano(),
	}
}

func (m WireMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}

func UnmarshalWireMessage(data []byte) (*WireMessage, error) {
	var msg WireMessage
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, err
	}
	return &msg, nil
}
