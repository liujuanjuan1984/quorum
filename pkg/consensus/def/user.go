package def

import (
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type User interface {
	AddBlock(block *quorumpb.Block) error
}
