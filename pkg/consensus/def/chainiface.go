package def

import (
	chaindef "github.com/rumsystem/quorum/internal/pkg/chainsdk/def"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type ChainSnowmanIface interface {
	GetTrxFactory() chaindef.TrxFactoryIface
	SaveChainInfoToDb() error
	ApplyTrxsFullNode(trxs []*quorumpb.Trx, acceptedBlockId uint64, nodename string) error
	ApplyTrxsProducerNode(trxs []*quorumpb.Trx, acceptedBlockId uint64, nodename string) error
	ApplyDueProducerUpdates(nextBlockId uint64, nodename string) (int, error)
	SetCurrEpoch(currEpoch uint64)
	IncCurrEpoch()
	GetCurrEpoch() uint64
	SetCurrBlockId(currBlock uint64)
	IncCurrBlockId()
	GetCurrBlockId() uint64
	SetLastUpdate(lastUpdate int64)
	GetLastUpdate() int64
}
