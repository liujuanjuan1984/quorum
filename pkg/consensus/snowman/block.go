package snowman

import (
	"bytes"
	"encoding/hex"
	"fmt"

	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type BlockStatus uint8

const (
	Unknown BlockStatus = iota
	Processing
	Accepted
	Rejected
)

func (s BlockStatus) String() string {
	switch s {
	case Unknown:
		return "unknown"
	case Processing:
		return "processing"
	case Accepted:
		return "accepted"
	case Rejected:
		return "rejected"
	default:
		return fmt.Sprintf("unknown-status-%d", s)
	}
}

func BlockID(block *quorumpb.Block) string {
	if block == nil || len(block.BlockHash) == 0 {
		return ""
	}
	return hex.EncodeToString(block.BlockHash)
}

func ParentID(block *quorumpb.Block) string {
	if block == nil || len(block.PrevHash) == 0 {
		return ""
	}
	return hex.EncodeToString(block.PrevHash)
}

func SameParent(a, b *quorumpb.Block) bool {
	if a == nil || b == nil {
		return false
	}
	return a.GroupId == b.GroupId &&
		a.BlockId == b.BlockId &&
		bytes.Equal(a.PrevHash, b.PrevHash)
}
