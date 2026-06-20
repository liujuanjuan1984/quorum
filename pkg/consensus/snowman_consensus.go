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
			_ = nodectx.GetNodeCtx().GetChainStorage().SaveSnowmanBlock(lastAccepted, snowman.Accepted.String(), p.nodename)
			_ = nodectx.GetNodeCtx().GetChainStorage().SetSnowmanLastAccepted(p.groupId, snowman.BlockID(lastAccepted), p.nodename)
		}
	} else if p.grpItem.GenesisBlock != nil {
		engine.SetLastAccepted(p.grpItem.GenesisBlock)
		_ = nodectx.GetNodeCtx().GetChainStorage().SaveSnowmanBlock(p.grpItem.GenesisBlock, snowman.Accepted.String(), p.nodename)
		_ = nodectx.GetNodeCtx().GetChainStorage().SetSnowmanLastAccepted(p.groupId, snowman.BlockID(p.grpItem.GenesisBlock), p.nodename)
	}
	p.params = params
	p.engine = engine
	return nil
}

func (p *SnowmanProducer) Start() {
	if p.engine == nil || !p.engine.IsValidator(p.grpItem.UserSignPubkey) {
		return
	}
	go func() {
		if err := p.requestAcceptedFrontier(); err != nil {
			snowmanLog.Debugf("<%s> accepted frontier request skipped: %s", p.groupId, err.Error())
		}
	}()
	ticker := time.NewTicker(time.Duration(p.params.ProposerWindowMs) * time.Millisecond)
	go func() {
		defer ticker.Stop()
		for {
			select {
			case <-p.stopCh:
				return
			case <-ticker.C:
				p.expireAndRepoll()
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
	if block.BlockId > 0 {
		parentExists, err := nodectx.GetNodeCtx().GetChainStorage().IsBlockExist(block.GroupId, block.ParentBlockId, false, p.nodename)
		if err != nil || !parentExists {
			if err := nodectx.GetNodeCtx().GetChainStorage().SaveSnowmanBlock(block, snowman.Processing.String(), p.nodename); err != nil {
				return err
			}
			return p.requestAncestors(block)
		}
	}
	blockID := snowman.BlockID(block)
	if status, err := nodectx.GetNodeCtx().GetChainStorage().GetSnowmanBlockStatus(block.GroupId, blockID, p.nodename); err != nil || status != snowman.Accepted.String() {
		if err := nodectx.GetNodeCtx().GetChainStorage().SaveSnowmanBlock(block, snowman.Processing.String(), p.nodename); err != nil {
			return err
		}
	}
	if err := p.engine.IssueBlock(block); err != nil {
		return err
	}
	if p.engine.Status(snowman.BlockID(block)) == snowman.Processing {
		return p.startPoll(block)
	}
	return nil
}

func (p *SnowmanProducer) HandleMessage(msg *quorumpb.SnowmanMessage) error {
	if msg == nil {
		return nil
	}
	if msg.GroupId != p.groupId {
		return nil
	}
	if !p.engine.IsValidator(msg.SenderPubkey) {
		return errors.New("snowman message sender is not an active validator")
	}
	if err := snowman.VerifyMessageSignature(msg); err != nil {
		return err
	}
	switch msg.Type {
	case quorumpb.SnowmanMessageType_PUT_BLOCK:
		return p.AddBlock(msg.Block)
	case quorumpb.SnowmanMessageType_GET_BLOCK:
		return p.sendBlock(msg.RequestId, msg.BlockId)
	case quorumpb.SnowmanMessageType_GET_ANCESTORS:
		return p.sendAncestors(msg.RequestId, msg.BlockId)
	case quorumpb.SnowmanMessageType_ANCESTORS:
		return p.handleAncestors(msg.Blocks)
	case quorumpb.SnowmanMessageType_PUSH_QUERY:
		if msg.Block != nil {
			if err := p.AddBlock(msg.Block); err != nil {
				return err
			}
		}
		return p.sendChits(msg.RequestId)
	case quorumpb.SnowmanMessageType_PULL_QUERY:
		return p.sendChits(msg.RequestId)
	case quorumpb.SnowmanMessageType_CHITS:
		return p.engine.RecordVote(msg.RequestId, msg.SenderPubkey, msg.PreferenceId)
	case quorumpb.SnowmanMessageType_GET_ACCEPTED_FRONTIER:
		return p.sendAcceptedFrontier(msg.RequestId)
	case quorumpb.SnowmanMessageType_ACCEPTED_FRONTIER:
		return p.handleAcceptedFrontier(msg.BlockIds)
	case quorumpb.SnowmanMessageType_GET_ACCEPTED:
		return p.sendAccepted(msg.RequestId, msg.BlockIds)
	case quorumpb.SnowmanMessageType_ACCEPTED:
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
	producerSetVersion, err := nodectx.GetNodeCtx().GetChainStorage().GetProducerSetVersion(p.groupId, p.nodename)
	if err != nil {
		return err
	}
	if block.ProducerSetVersion != producerSetVersion {
		return fmt.Errorf("block producer set version %d does not match active version %d", block.ProducerSetVersion, producerSetVersion)
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
		if err := p.cIface.ApplyTrxsProducerNode(block.Trxs, block.BlockId, p.nodename); err != nil {
			return err
		}
	} else if err := p.cIface.ApplyTrxsFullNode(block.Trxs, block.BlockId, p.nodename); err != nil {
		return err
	}
	appliedProducerUpdates, err := p.cIface.ApplyDueProducerUpdates(block.BlockId+1, p.nodename)
	if err != nil {
		return err
	}
	for _, trx := range block.Trxs {
		_ = p.txBuffer.Delete(trx.TrxId)
	}
	p.cIface.SetCurrBlockId(block.BlockId)
	p.cIface.SetCurrEpoch(block.Epoch)
	p.cIface.SetLastUpdate(block.TimeStamp)
	if err := p.cIface.SaveChainInfoToDb(); err != nil {
		return err
	}
	if appliedProducerUpdates > 0 {
		return p.ReloadValidatorSet()
	}
	return nil
}

func (p *SnowmanProducer) RejectBlock(block *quorumpb.Block) error {
	if block == nil {
		return nil
	}
	for _, trx := range block.Trxs {
		if trx != nil {
			_ = p.txBuffer.Push(trx)
		}
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
	producerSetVersion, err := nodectx.GetNodeCtx().GetChainStorage().GetProducerSetVersion(p.groupId, p.nodename)
	if err != nil {
		return err
	}
	window := snowman.ProposerFor(parent.BlockHash, parent.BlockId+1, producerSetVersion, p.engine.Validators(), time.Now(), p.params)
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
	block, err := rumchaindata.CreateBlockByEthKey(parent, p.cIface.GetCurrEpoch()+1, trxs, false, producerSetVersion, window.Index, p.grpItem.UserSignPubkey, localcrypto.GetKeystore(), "", p.nodename)
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

func (p *SnowmanProducer) expireAndRepoll() {
	if p.engine == nil {
		return
	}
	p.engine.ExpirePolls(time.Now())
	if p.engine.OutstandingPolls() >= p.params.ConcurrentRepolls {
		return
	}
	block := p.engine.ProcessingPreference()
	if block == nil {
		return
	}
	if err := p.startPoll(block); err != nil {
		snowmanLog.Debugf("<%s> repoll skipped: %s", p.groupId, err.Error())
	}
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
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_PUSH_QUERY, requestID, p.grpItem.UserSignPubkey)
	msg.BlockId = snowman.BlockID(block)
	msg.Block = block
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	if err := p.broadcastMessage(msg); err != nil {
		return err
	}
	// Count the local validator's own preference when it was sampled.
	if poll.Sampled[p.grpItem.UserSignPubkey] {
		return p.engine.RecordVote(requestID, p.grpItem.UserSignPubkey, p.engine.Chits())
	}
	return nil
}

func (p *SnowmanProducer) sendChits(requestID string) error {
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_CHITS, requestID, p.grpItem.UserSignPubkey)
	msg.PreferenceId = p.engine.Chits()
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) sendAcceptedFrontier(requestID string) error {
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_ACCEPTED_FRONTIER, requestID, p.grpItem.UserSignPubkey)
	msg.PreferenceId = p.engine.LastAccepted()
	if msg.PreferenceId != "" {
		msg.BlockIds = []string{msg.PreferenceId}
	}
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) requestAcceptedFrontier() error {
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_GET_ACCEPTED_FRONTIER, fmt.Sprintf("%s-%d", p.grpItem.UserSignPubkey, time.Now().UnixNano()), p.grpItem.UserSignPubkey)
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) handleAcceptedFrontier(blockIDs []string) error {
	for _, blockID := range blockIDs {
		if blockID == "" {
			continue
		}
		status, err := nodectx.GetNodeCtx().GetChainStorage().GetSnowmanBlockStatus(p.groupId, blockID, p.nodename)
		if err == nil && status == snowman.Accepted.String() {
			continue
		}
		return p.requestAncestorsByID(blockID)
	}
	return nil
}

func (p *SnowmanProducer) broadcastSnowmanBlock(block *quorumpb.Block) error {
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_PUT_BLOCK, "", p.grpItem.UserSignPubkey)
	msg.BlockId = snowman.BlockID(block)
	msg.Block = block
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) requestAncestors(block *quorumpb.Block) error {
	return p.requestAncestorsByID(snowman.ParentID(block))
}

func (p *SnowmanProducer) requestAncestorsByID(blockID string) error {
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_GET_ANCESTORS, fmt.Sprintf("%s-%d", p.grpItem.UserSignPubkey, time.Now().UnixNano()), p.grpItem.UserSignPubkey)
	msg.BlockId = blockID
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) sendBlock(requestID string, blockID string) error {
	block, err := nodectx.GetNodeCtx().GetChainStorage().GetSnowmanBlockByHash(p.groupId, blockID, p.nodename)
	if err != nil {
		return nil
	}
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_PUT_BLOCK, requestID, p.grpItem.UserSignPubkey)
	msg.BlockId = blockID
	msg.Block = block
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) sendAncestors(requestID string, blockID string) error {
	blocks := p.collectAncestors(blockID, 32)
	if len(blocks) == 0 {
		return nil
	}
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_ANCESTORS, requestID, p.grpItem.UserSignPubkey)
	msg.BlockId = blockID
	msg.Blocks = blocks
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) handleAncestors(blocks []*quorumpb.Block) error {
	for i := len(blocks) - 1; i >= 0; i-- {
		if blocks[i] == nil {
			continue
		}
		if err := p.AddBlock(blocks[i]); err != nil {
			return err
		}
	}
	return nil
}

func (p *SnowmanProducer) sendAccepted(requestID string, blockIDs []string) error {
	accepted := make([]string, 0, len(blockIDs))
	for _, blockID := range blockIDs {
		status, err := nodectx.GetNodeCtx().GetChainStorage().GetSnowmanBlockStatus(p.groupId, blockID, p.nodename)
		if err == nil && status == snowman.Accepted.String() {
			accepted = append(accepted, blockID)
		}
	}
	msg := snowman.NewMessage(p.groupId, quorumpb.SnowmanMessageType_ACCEPTED, requestID, p.grpItem.UserSignPubkey)
	msg.BlockIds = accepted
	if err := snowman.SignMessage(msg, p.nodename); err != nil {
		return err
	}
	return p.broadcastMessage(msg)
}

func (p *SnowmanProducer) collectAncestors(blockID string, limit int) []*quorumpb.Block {
	if limit <= 0 {
		return nil
	}
	blocks := []*quorumpb.Block{}
	currentID := blockID
	for currentID != "" && len(blocks) < limit {
		block, err := nodectx.GetNodeCtx().GetChainStorage().GetSnowmanBlockByHash(p.groupId, currentID, p.nodename)
		if err != nil {
			break
		}
		blocks = append(blocks, block)
		currentID = snowman.ParentID(block)
	}
	return blocks
}

func (p *SnowmanProducer) broadcastMessage(msg *quorumpb.SnowmanMessage) error {
	if cmgr, err := conn.GetConn().GetConnMgr(p.groupId); err == nil {
		return cmgr.BroadcastSnowmanMessage(msg)
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
