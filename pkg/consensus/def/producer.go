package def

import (
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type Producer interface {
	AddTrx(trx *quorumpb.Trx) error
	AddBlock(block *quorumpb.Block) error
	HandleMessage(msg *quorumpb.SnowmanMessage) error
	ReloadValidatorSet() error
	Start()
	Stop()
}
