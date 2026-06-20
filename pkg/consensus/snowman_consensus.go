package consensus

import (
	"errors"
	"fmt"
	"time"

	"github.com/rumsystem/quorum/internal/pkg/conn"
	rumerrors "github.com/rumsystem/quorum/internal/pkg/errors"
	"github.com/rumsystem/quorum/internal/pkg/logging"
	"github.com/rumsystem/quorum/internal/pkg/nodectx"
	"github.com/rumsystem/quorum/pkg/consensus/def"
	"github.com/rumsystem/quorum/pkg/consensus/snowman"
	localcrypto "github.com/rumsystem/quorum/pkg/crypto"
	rumchaindata "github.com/rumsystem/quorum/pkg/data"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

var snowmanLog = logging.Logger("snowman")

type SnowmanConsensus struct {
	producer def.Producer
	user     def.User
}

func NewSnowmanConsensus(p def.Producer, u def.User) *SnowmanConsensus {
	return &SnowmanConsensus{producer: p, user: u}
}

func (s *SnowmanConsensus) Name() string {
	return "Snowman++"
}

func (s *SnowmanConsensus) Producer() def.Producer {
	return s.producer
}

func (s *SnowmanConsensus) User() def.User {
	return s.user
}

func (s *SnowmanConsensus) SetProducer(p def.Producer) {
	s.producer = p
}

func (s *SnowmanConsensus) SetUser(u def.User) {
	s.user = u
}

func (s *SnowmanConsensus) Start() {
	if s.producer != nil {
		s.producer.Start()
	}
}

func (s *SnowmanConsensus) Stop() {
	if s.producer != nil {
		s.producer.Stop()
	}
}

type SnowmanProducer struct {
	grpItem  *quorumpb.GroupItem
	nodename string
	cIface   def.ChainSnowmanIface
	groupId  string
	params   snowman.Params
	engine   *snowman.Engine
	txBuffer *TrxBuffer
	stopCh   chan struct{}
}

func NewSnowmanProducer(item *quorumpb.GroupItem, nodename string, iface def.ChainSnowmanIface) (*SnowmanProducer, error) {
	p := &SnowmanProducer{
		grpItem:  item,
		nodename: nodename,
		cIface:   iface,
		groupId:  item.GroupId,
		txBuffer: NewTrxBuffer(item.GroupId),
		stopCh:   make(chan struct{}),
	}
	if err := p.ReloadValidatorSet(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *SnowmanProducer) ReloadValidatorSet() error {
	producers, err := nodectx.GetNodeCtx().GetChainStorage().GetProducers(p.groupId, p.nodename)
	if err != nil {
		return err
	}
	nodes := make([]string, 0, len(producers))
	for _, producer := range producers {
		nodes = append(nodes, producer.ProducerPubkey)
	}
	params := snowman.DefaultParams(len(nodes))
	engine, err := snowman.NewEngine(p.groupId, p.grpItem.UserSignPubkey, nodes, params, p)
	if err != nil {
		return err
	}
	if currBlock := p.cIface.GetCurrBlockId(); currBlock > 0 {
		if lastAccepted, err := nodectx.GetNodeCtx().GetChainStorage().GetBlock(p.groupId, currBlock, false, p.nodename); err == nil {
			engine.SetLastAccepted(lastAccepted)
		}
	} else if p.grpItem.GenesisBlock != nil {
		engine.SetLastAccepted(p.grpItem.GenesisBlock)
	}
	p.params = params
	p.engine = engine
	return nil
}

func (p *SnowmanProducer) Start() {
	if p.engine == nil || !p.engine.IsValidator(p.grpItem.UserSignPubkey) {
		return
	}
	ticker := time.NewTicker(time.Duration(p.params.ProposerWindowMs) * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				if err := p.tryPropose(); err != nil {
					snowmanLog.Debugf("<%s> propose skipped: %s", p.groupId, err.Error())
				}
			}
		}
	}()
}

func (p *SnowmanProducer) Stop() {
	select {
	case <-p.stopCh:
	default:
		close(p.stopCh)
	}
}

func (p *SnowmanProducer) AddTrx(trx *quorumpb.Trx) error {
	if trx == nil {
		return nil
	}
	isAllow, err := nodectx.GetNodeCtx().GetChainStorage().CheckTrxTypeAuth(trx.GroupId, trx.SenderPubkey, trx.Type, p.nodename)
	if err != nil {
		return err
	}
	if !isAllow {
		return nil
	}
	return p.txBuffer.Push(trx)
}

func (p *SnowmanProducer) AddBlock(block *quorumpb.Block) error {
	if p.engine == nil {
		return errors.New("snowman engine is not initialized")
	}
	if err := p.engine.IssueBlock(block); err != nil {
		return err
	}
	if p.engine.Status(snowman.BlockID(block)) == snowman.Processing {
		return p.startPoll(block)
	}
	return nil
}

func (p *SnowmanProducer) HandleMessage(msg *snowman.WireMessage) error {
	if msg == nil {
		return nil
	}
	if msg.GroupID != p.groupId {
		return nil
	}
	switch msg.Type {
	case snowman.MessagePutBlock:
		return p.AddBlock(msg.Block)
	case snowman.MessagePushQuery:
		if msg.Block != nil {
			if err := p.AddBlock(msg.Block); err != nil {
				return err
			}
		}
		return p.sendChits(msg.RequestID)
	case snowman.MessagePullQuery:
		return p.sendChits(msg.RequestID)
	case snowman.MessageChits:
		return p.engine.RecordVote(msg.RequestID, msg.SenderPubkey, msg.PreferenceID)
	case snowman.MessageGetAcceptedFrontier:
		return p.sendAcceptedFrontier(msg.RequestID)
	case snowman.MessageAcceptedFrontier:
		return nil
	default:
		return nil
	}
}

func (p *SnowmanProducer) VerifyBlock(block *quorumpb.Block) error {
	if block == nil {
		return errors.New("block is nil")
	}
	if !p.engine.IsValidator(block.ProducerPubkey) {
		return errors.New("block producer is not in active validator set")
	}
	parentID := block.BlockId - 1
	parent, err := nodectx.GetNodeCtx().GetChainStorage().GetBlock(block.GroupId, parentID, false, p.nodename)
	if err != nil {
		return err
	}
	valid, err := rumchaindata.ValidBlockWithParent(block, parent)
	if err != nil {
		return err
	}
	if !valid {
		return errors.New("block signature or parent linkage is invalid")
	}
	return nil
}

func (p *SnowmanProducer) AcceptBlock(block *quorumpb.Block) error {
	if err := nodectx.GetNodeCtx().GetChainStorage().SaveSnowmanBlock(block, snowman.Accepted.String(), p.nodename); err != nil {
		return err
	}
	if err := nodectx.GetNodeCtx().GetChainStorage().SetSnowmanLastAccepted(block.GroupId, snowman.BlockID(block), p.nodename); err != nil {
		return err
	}
	exists, err := nodectx.GetNodeCtx().GetChainStorage().IsBlockExist(block.GroupId, block.BlockId, false, p.nodename)
	if err != nil {
		return err
	}
	if !exists {
		if err := nodectx.GetNodeCtx().GetChainStorage().AddBlock(block, false, p.nodename); err != nil && err != rumerrors.ErrBlockExist {
			return err
		}
	}
	if nodectx.GetNodeCtx().NodeType == nodectx.PRODUCER_NODE {
		if err := p.cIface.ApplyTrxsProducerNode(block.Trxs, p.nodename); err != nil {
			return err
		}
	} else if err := p.cIface.ApplyTrxsFullNode(block.Trxs, p.nodename); err != nil {
		return err
	}
	for _, trx := range block.Trxs {
		_ = p.txBuffer.Delete(trx.TrxId)
	}
	p.cIface.SetCurrBlockId(block.BlockId)
	p.cIface.SetCurrEpoch(block.Epoch)
	p.cIface.SetLastUpdate(block.TimeStamp)
	return p.cIface.SaveChainInfoToDb()
}

func (p *SnowmanProducer) RejectBlock(block *quorumpb.Block) error {
	if block == nil {
		return nil
	}
	return nodectx.GetNodeCtx().GetChainStorage().SaveSnowmanBlock(block, snowman.Rejected.String(), p.nodename)
}

func (p *SnowmanProducer) tryPropose() error {
	if p.engine == nil || !p.engine.IsValidator(p.grpItem.UserSignPubkey) {
		return errors.New("local node is not an active validator")
	}
	parent, err := nodectx.GetNodeCtx().GetChainStorage().GetBlock(p.groupId, p.cIface.GetCurrBlockId(), false, p.nodename)
	if err != nil {
		return err
	}
	window := snowman.ProposerFor(parent.BlockHash, parent.BlockId+1, 0, p.engine.Validators(), time.Now(), p.params)
	if !window.Open && window.Validator != p.grpItem.UserSignPubkey {
		return errors.New("outside local proposer window")
	}
	trxs, err := p.txBuffer.GetNRandTrx(p.params.MaxBlockTransactions)
	if err != nil {
		return err
	}
	if len(trxs) == 0 {
		return nil
	}
	block, err := rumchaindata.CreateBlockByEthKey(parent, p.cIface.GetCurrEpoch()+1, trxs, false, p.grpItem.UserSignPubkey, localcrypto.GetKeystore(), "", p.nodename)
	if err != nil {
		return err
	}
	if err := p.engine.IssueBlock(block); err != nil {
		return err
	}
	if err := p.broadcastSnowmanBlock(block); err != nil {
		return err
	}
	return p.startPoll(block)
}

func (p *SnowmanProducer) startPoll(block *quorumpb.Block) error {
	if p.engine == nil {
		return nil
	}
	requestID := fmt.Sprintf("%s-%d", p.grpItem.UserSignPubkey, time.Now().UnixNano())
	poll, err := p.engine.NewPoll(requestID, []string{snowman.BlockID(block)})
	if err != nil {
		return err
	}
	msg := snowman.NewWireMessage(p.groupId, snowman.MessagePushQuery, requestID, p.grpItem.UserSignPubkey)
	msg.BlockID = snowman.BlockID(block)
	msg.Block = block
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	if cmgr, err := conn.GetConn().GetConnMgr(p.groupId); err == nil {
		if err := cmgr.BroadcastSnowmanMessage(data); err != nil {
			return err
		}
	}
	// Count the local validator's own preference when it was sampled.
	if poll.Sampled[p.grpItem.UserSignPubkey] {
		return p.engine.RecordVote(requestID, p.grpItem.UserSignPubkey, p.engine.Chits())
	}
	return nil
}

func (p *SnowmanProducer) sendChits(requestID string) error {
	msg := snowman.NewWireMessage(p.groupId, snowman.MessageChits, requestID, p.grpItem.UserSignPubkey)
	msg.PreferenceID = p.engine.Chits()
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	if cmgr, err := conn.GetConn().GetConnMgr(p.groupId); err == nil {
		return cmgr.BroadcastSnowmanMessage(data)
	}
	return nil
}

func (p *SnowmanProducer) sendAcceptedFrontier(requestID string) error {
	msg := snowman.NewWireMessage(p.groupId, snowman.MessageAcceptedFrontier, requestID, p.grpItem.UserSignPubkey)
	msg.PreferenceID = p.engine.LastAccepted()
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	if cmgr, err := conn.GetConn().GetConnMgr(p.groupId); err == nil {
		return cmgr.BroadcastSnowmanMessage(data)
	}
	return nil
}

func (p *SnowmanProducer) broadcastSnowmanBlock(block *quorumpb.Block) error {
	msg := snowman.NewWireMessage(p.groupId, snowman.MessagePutBlock, "", p.grpItem.UserSignPubkey)
	msg.BlockID = snowman.BlockID(block)
	msg.Block = block
	data, err := msg.Marshal()
	if err != nil {
		return err
	}
	if cmgr, err := conn.GetConn().GetConnMgr(p.groupId); err == nil {
		return cmgr.BroadcastSnowmanMessage(data)
	}
	return nil
}

type SnowmanUser struct {
	producer *SnowmanProducer
}

func NewSnowmanUser(producer *SnowmanProducer) *SnowmanUser {
	return &SnowmanUser{producer: producer}
}

func (u *SnowmanUser) AddBlock(block *quorumpb.Block) error {
	if u == nil || u.producer == nil {
		return errors.New("snowman user has no engine")
	}
	return u.producer.AddBlock(block)
}
