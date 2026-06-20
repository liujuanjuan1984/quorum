package snowman

import (
	"testing"

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
	if err := engine.IssueBlock(a); err != nil {
		t.Fatal(err)
	}
	if err := engine.IssueBlock(b); err != nil {
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
