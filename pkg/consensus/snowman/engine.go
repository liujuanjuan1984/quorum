package snowman

import (
	"fmt"
	"sync"
	"time"

	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type Adapter interface {
	VerifyBlock(block *quorumpb.Block) error
	AcceptBlock(block *quorumpb.Block) error
	RejectBlock(block *quorumpb.Block) error
}

type Engine struct {
	mu           sync.Mutex
	groupID      string
	myPubkey     string
	params       Params
	validators   *ValidatorSet
	adapter      Adapter
	blocks       map[string]*quorumpb.Block
	children     map[string][]string
	status       map[string]BlockStatus
	preference   string
	confidence   map[string]int
	polls        map[string]*Poll
	lastAccepted string
}

func NewEngine(groupID, myPubkey string, validators []string, params Params, adapter Adapter) (*Engine, error) {
	vset := NewValidatorSet(validators)
	if params.K == 0 {
		params = DefaultParams(vset.Len())
	}
	if err := params.Validate(vset.Len()); err != nil {
		return nil, err
	}
	return &Engine{
		groupID:    groupID,
		myPubkey:   myPubkey,
		params:     params,
		validators: vset,
		adapter:    adapter,
		blocks:     map[string]*quorumpb.Block{},
		children:   map[string][]string{},
		status:     map[string]BlockStatus{},
		confidence: map[string]int{},
		polls:      map[string]*Poll{},
	}, nil
}

func (e *Engine) SetLastAccepted(block *quorumpb.Block) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := BlockID(block)
	if id == "" {
		return
	}
	e.blocks[id] = block
	e.status[id] = Accepted
	e.preference = id
	e.lastAccepted = id
}

func (e *Engine) LastAccepted() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.lastAccepted
}

func (e *Engine) Preference() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.preference
}

func (e *Engine) Status(blockID string) BlockStatus {
	e.mu.Lock()
	defer e.mu.Unlock()
	if status, ok := e.status[blockID]; ok {
		return status
	}
	return Unknown
}

func (e *Engine) Validators() []string {
	return e.validators.Nodes()
}

func (e *Engine) Params() Params {
	return e.params
}

func (e *Engine) OutstandingPolls() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.polls)
}

func (e *Engine) IsValidator(pubkey string) bool {
	return e.validators.Contains(pubkey)
}

func (e *Engine) IssueBlock(block *quorumpb.Block) error {
	if block == nil {
		return fmt.Errorf("block is nil")
	}
	if !e.validators.Contains(block.ProducerPubkey) {
		return fmt.Errorf("producer %s is not in active validator set", block.ProducerPubkey)
	}
	if e.adapter != nil {
		if err := e.adapter.VerifyBlock(block); err != nil {
			return err
		}
	}

	e.mu.Lock()
	id := BlockID(block)
	if id == "" {
		e.mu.Unlock()
		return fmt.Errorf("block has empty hash")
	}
	if e.status[id] == Accepted || e.status[id] == Processing {
		e.mu.Unlock()
		return nil
	}
	parentID := ParentID(block)
	e.blocks[id] = block
	e.status[id] = Processing
	e.children[parentID] = append(e.children[parentID], id)
	if e.preference == "" || e.status[e.preference] != Processing {
		e.preference = id
	}
	e.mu.Unlock()

	if e.validators.Len() == 1 {
		return e.accept(id)
	}
	return nil
}

func (e *Engine) NewPoll(requestID string, options []string) (*Poll, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.polls) >= e.params.MaxOutstandingPolls {
		return nil, fmt.Errorf("too many outstanding polls")
	}
	sampled := e.validators.Sample(e.params.K)
	poll := NewPoll(requestID, sampled, options, time.Duration(e.params.RoundTimeoutMs)*time.Millisecond)
	e.polls[requestID] = poll
	return poll, nil
}

func (e *Engine) RecordVote(requestID, validator, preference string) error {
	e.mu.Lock()
	poll, ok := e.polls[requestID]
	if !ok {
		e.mu.Unlock()
		return fmt.Errorf("poll %s not found", requestID)
	}
	if err := poll.AddVote(validator, preference); err != nil {
		e.mu.Unlock()
		return err
	}
	if !poll.Finished() {
		e.mu.Unlock()
		return nil
	}
	delete(e.polls, requestID)
	winner, count := poll.Result()
	e.mu.Unlock()
	return e.applyPollResult(winner, count)
}

func (e *Engine) applyPollResult(winner string, count int) error {
	if winner == "" {
		return nil
	}
	e.mu.Lock()
	if count >= e.params.AlphaPreference {
		e.preference = winner
	}
	if count < e.params.AlphaConfidence {
		e.confidence[winner] = 0
		e.mu.Unlock()
		return nil
	}
	e.confidence[winner]++
	confidence := e.confidence[winner]
	block := e.blocks[winner]
	beta := e.params.BetaVirtuous
	if e.hasConflictLocked(winner) {
		beta = e.params.BetaRogue
	}
	e.mu.Unlock()

	if block != nil && confidence >= beta {
		return e.accept(winner)
	}
	return nil
}

func (e *Engine) Chits() string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.preference
}

func (e *Engine) ProcessingPreference() *quorumpb.Block {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.preference == "" || e.status[e.preference] != Processing {
		return nil
	}
	return e.blocks[e.preference]
}

func (e *Engine) ExpirePolls(now time.Time) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	expired := 0
	for requestID, poll := range e.polls {
		if now.After(poll.Deadline) {
			delete(e.polls, requestID)
			expired++
		}
	}
	return expired
}

func (e *Engine) accept(blockID string) error {
	e.mu.Lock()
	block := e.blocks[blockID]
	if block == nil {
		e.mu.Unlock()
		return fmt.Errorf("block %s not found", blockID)
	}
	if e.status[blockID] == Accepted {
		e.mu.Unlock()
		return nil
	}
	e.status[blockID] = Accepted
	e.preference = blockID
	e.lastAccepted = blockID
	conflicts := e.conflictingChildrenLocked(blockID)
	for _, conflictID := range conflicts {
		e.status[conflictID] = Rejected
	}
	e.mu.Unlock()

	if e.adapter != nil {
		if err := e.adapter.AcceptBlock(block); err != nil {
			return err
		}
		for _, conflictID := range conflicts {
			if conflict := e.blocks[conflictID]; conflict != nil {
				_ = e.adapter.RejectBlock(conflict)
			}
		}
	}
	return nil
}

func (e *Engine) hasConflictLocked(blockID string) bool {
	block := e.blocks[blockID]
	if block == nil {
		return false
	}
	for _, childID := range e.children[ParentID(block)] {
		if childID != blockID && e.status[childID] == Processing {
			return true
		}
	}
	return false
}

func (e *Engine) conflictingChildrenLocked(blockID string) []string {
	block := e.blocks[blockID]
	if block == nil {
		return nil
	}
	conflicts := []string{}
	for _, childID := range e.children[ParentID(block)] {
		if childID != blockID && e.status[childID] == Processing {
			conflicts = append(conflicts, childID)
		}
	}
	return conflicts
}
