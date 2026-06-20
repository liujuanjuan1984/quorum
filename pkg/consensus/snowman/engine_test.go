package snowman

import (
	"testing"
	"time"

	quorumpb "github.com/rumsystem/quorum/pkg/pb"
)

type testAdapter struct {
	accepted []string
	rejected []string
}

func (a *testAdapter) VerifyBlock(block *quorumpb.Block) error {
	return nil
}

func (a *testAdapter) AcceptBlock(block *quorumpb.Block) error {
	a.accepted = append(a.accepted, BlockID(block))
	return nil
}

func (a *testAdapter) RejectBlock(block *quorumpb.Block) error {
	a.rejected = append(a.rejected, BlockID(block))
	return nil
}

func TestSingleValidatorAcceptsIssuedBlock(t *testing.T) {
	adapter := &testAdapter{}
	engine, err := NewEngine("group", "a", []string{"a"}, DefaultParams(1), adapter)
	if err != nil {
		t.Fatal(err)
	}
	block := testBlock("group", "a", 1, []byte("parent"), []byte("block-a"))
	if err := engine.IssueBlock(block); err != nil {
		t.Fatal(err)
	}
	if engine.Status(BlockID(block)) != Accepted {
		t.Fatalf("expected accepted, got %s", engine.Status(BlockID(block)))
	}
	if len(adapter.accepted) != 1 {
		t.Fatalf("expected one accepted callback, got %d", len(adapter.accepted))
	}
}

func TestPollAcceptance(t *testing.T) {
	adapter := &testAdapter{}
	params := DefaultParams(3)
	params.K = 3
	params.AlphaPreference = 2
	params.AlphaConfidence = 2
	params.BetaVirtuous = 1
	params.BetaRogue = 1
	engine, err := NewEngine("group", "a", []string{"a", "b", "c"}, params, adapter)
	if err != nil {
		t.Fatal(err)
	}
	block := testBlock("group", "a", 1, []byte("parent"), []byte("block-a"))
	if err := engine.IssueBlock(block); err != nil {
		t.Fatal(err)
	}
	poll, err := engine.NewPoll("poll-1", []string{BlockID(block)})
	if err != nil {
		t.Fatal(err)
	}
	for validator := range poll.Sampled {
		if err := engine.RecordVote("poll-1", validator, BlockID(block)); err != nil {
			t.Fatal(err)
		}
	}
	if engine.Status(BlockID(block)) != Accepted {
		t.Fatalf("expected accepted, got %s", engine.Status(BlockID(block)))
	}
}

func TestAcceptRejectsConflictingSibling(t *testing.T) {
	adapter := &testAdapter{}
	params := DefaultParams(3)
	params.K = 3
	params.AlphaPreference = 2
	params.AlphaConfidence = 2
	params.BetaVirtuous = 1
	params.BetaRogue = 1
	engine, err := NewEngine("group", "a", []string{"a", "b", "c"}, params, adapter)
	if err != nil {
		t.Fatal(err)
	}
	a := testBlock("group", "a", 1, []byte("parent"), []byte("block-a"))
	b := testBlock("group", "b", 1, []byte("parent"), []byte("block-b"))
	bChild := testBlock("group", "b", 2, []byte("block-b"), []byte("block-b-child"))
	if err := engine.IssueBlock(a); err != nil {
		t.Fatal(err)
	}
	if err := engine.IssueBlock(b); err != nil {
		t.Fatal(err)
	}
	if err := engine.IssueBlock(bChild); err != nil {
		t.Fatal(err)
	}
	poll, err := engine.NewPoll("poll-1", []string{BlockID(a), BlockID(b)})
	if err != nil {
		t.Fatal(err)
	}
	for validator := range poll.Sampled {
		if err := engine.RecordVote("poll-1", validator, BlockID(a)); err != nil {
			t.Fatal(err)
		}
	}
	if engine.Status(BlockID(a)) != Accepted {
		t.Fatalf("expected accepted, got %s", engine.Status(BlockID(a)))
	}
	if engine.Status(BlockID(b)) != Rejected {
		t.Fatalf("expected rejected, got %s", engine.Status(BlockID(b)))
	}
	if engine.Status(BlockID(bChild)) != Rejected {
		t.Fatalf("expected rejected descendant, got %s", engine.Status(BlockID(bChild)))
	}
	if len(adapter.rejected) != 2 {
		t.Fatalf("expected two rejected callbacks, got %d", len(adapter.rejected))
	}
}

func TestPollRejectsInvalidAndDuplicateVotes(t *testing.T) {
	poll := NewPoll("poll-1", []string{"a", "b"}, []string{"block-a"}, time.Second)
	if err := poll.AddVote("a", "unknown-block"); err == nil {
		t.Fatal("expected invalid preference to be rejected")
	}
	if err := poll.AddVote("not-sampled", "block-a"); err == nil {
		t.Fatal("expected non-sampled validator vote to be rejected")
	}
	if err := poll.AddVote("a", "block-a"); err != nil {
		t.Fatal(err)
	}
	if err := poll.AddVote("a", "block-a"); err == nil {
		t.Fatal("expected duplicate validator vote to be rejected")
	}
}

func TestExpirePollsClearsOutstandingPolls(t *testing.T) {
	engine, err := NewEngine("group", "a", []string{"a", "b", "c"}, DefaultParams(3), &testAdapter{})
	if err != nil {
		t.Fatal(err)
	}
	poll, err := engine.NewPoll("poll-1", []string{"block-a"})
	if err != nil {
		t.Fatal(err)
	}
	poll.Deadline = time.Now().Add(-time.Second)
	if expired := engine.ExpirePolls(time.Now()); expired != 1 {
		t.Fatalf("expected one expired poll, got %d", expired)
	}
	if outstanding := engine.OutstandingPolls(); outstanding != 0 {
		t.Fatalf("expected no outstanding polls, got %d", outstanding)
	}
}

func TestFourValidatorsAcceptWithSampledQuorum(t *testing.T) {
	adapter := &testAdapter{}
	params := DefaultParams(4)
	params.K = 3
	params.AlphaPreference = 2
	params.AlphaConfidence = 2
	params.BetaVirtuous = 1
	params.BetaRogue = 1
	engine, err := NewEngine("group", "a", []string{"a", "b", "c", "d"}, params, adapter)
	if err != nil {
		t.Fatal(err)
	}
	block := testBlock("group", "a", 1, []byte("parent"), []byte("block-a"))
	if err := engine.IssueBlock(block); err != nil {
		t.Fatal(err)
	}
	poll, err := engine.NewPoll("poll-1", []string{BlockID(block)})
	if err != nil {
		t.Fatal(err)
	}
	for validator := range poll.Sampled {
		if err := engine.RecordVote("poll-1", validator, BlockID(block)); err != nil {
			t.Fatal(err)
		}
	}
	if engine.Status(BlockID(block)) != Accepted {
		t.Fatalf("expected accepted, got %s", engine.Status(BlockID(block)))
	}
}

func testBlock(groupID, producer string, height uint64, parentHash []byte, hash []byte) *quorumpb.Block {
	return &quorumpb.Block{
		GroupId:        groupID,
		BlockId:        height,
		Epoch:          height,
		PrevHash:       parentHash,
		ProducerPubkey: producer,
		BlockHash:      hash,
	}
}
