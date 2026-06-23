package chainstorage

import (
	"context"
	"testing"

	storage "github.com/rumsystem/quorum/internal/pkg/storage"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
	"google.golang.org/protobuf/proto"
)

func TestApplyDueProducerUpdatesHonorsEffectiveBlockId(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	groupStore, err := storage.NewStore(ctx, dir, "groupinfo")
	if err != nil {
		t.Fatal(err)
	}
	defer groupStore.Close()
	chainStore, err := storage.NewStore(ctx, dir, "chain")
	if err != nil {
		t.Fatal(err)
	}
	defer chainStore.Close()

	cs := NewChainStorage(&storage.DbMgr{
		GroupInfoDb: groupStore,
		Db:          chainStore,
	})
	groupID := "group"
	owner := "owner"
	if err := cs.AddGroup(&quorumpb.GroupItem{GroupId: groupID, OwnerPubKey: owner}); err != nil {
		t.Fatal(err)
	}
	if err := cs.AddProducer(&quorumpb.ProducerItem{
		GroupId:          groupID,
		GroupOwnerPubkey: owner,
		ProducerPubkey:   owner,
	}); err != nil {
		t.Fatal(err)
	}

	bundle := &quorumpb.ValidatorBundleItem{
		EffectiveBlockId: 4,
		Producers: []*quorumpb.ProducerItem{{
			GroupId:          groupID,
			GroupOwnerPubkey: owner,
			ProducerPubkey:   "validator-b",
		}},
	}
	data, err := proto.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}
	trx := &quorumpb.Trx{GroupId: groupID, TrxId: "producer-trx", Data: data}
	if err := cs.UpdateProducerTrx(trx, 1); err != nil {
		t.Fatal(err)
	}

	applied, err := cs.ApplyDueProducerUpdates(groupID, 3)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("expected no due producer updates, got %d", applied)
	}
	producers, err := cs.GetProducers(groupID)
	if err != nil {
		t.Fatal(err)
	}
	if len(producers) != 1 || producers[0].ProducerPubkey != owner {
		t.Fatalf("producer set changed before effective height: %+v", producers)
	}

	applied, err = cs.ApplyDueProducerUpdates(groupID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 1 {
		t.Fatalf("expected one due producer update, got %d", applied)
	}
	version, err := cs.GetProducerSetVersion(groupID)
	if err != nil {
		t.Fatal(err)
	}
	if version != 4 {
		t.Fatalf("expected producer set version 4, got %d", version)
	}
	snapshot, err := cs.GetProducerSetSnapshot(groupID, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot) != 2 {
		t.Fatalf("expected producer snapshot owner plus validator, got %+v", snapshot)
	}
	producers, err = cs.GetProducers(groupID)
	if err != nil {
		t.Fatal(err)
	}
	if len(producers) != 2 {
		t.Fatalf("expected owner plus validator, got %+v", producers)
	}
	found := map[string]bool{}
	for _, producer := range producers {
		found[producer.ProducerPubkey] = true
	}
	if !found[owner] || !found["validator-b"] {
		t.Fatalf("expected active owner and validator-b, got %+v", producers)
	}

	applied, err = cs.ApplyDueProducerUpdates(groupID, 5)
	if err != nil {
		t.Fatal(err)
	}
	if applied != 0 {
		t.Fatalf("expected pending update to be consumed once, got %d", applied)
	}
}
