package def

import (
	"github.com/rumsystem/quorum/pkg/consensus/snowman"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type Producer interface {
	AddTrx(trx *quorumpb.Trx) error
	AddBlock(block *quorumpb.Block) error
	HandleMessage(msg *snowman.WireMessage) error
	ReloadValidatorSet() error
	Start()
	Stop()
}
