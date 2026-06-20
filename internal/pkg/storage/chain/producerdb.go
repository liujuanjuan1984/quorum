package chainstorage

import (
	"sort"
	"strconv"
	"strings"

	s "github.com/rumsystem/quorum/internal/pkg/storage"
	"github.com/rumsystem/quorum/internal/pkg/storage/def"
	localcrypto "github.com/rumsystem/quorum/pkg/crypto"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
	"google.golang.org/protobuf/proto"
)

type pendingValidatorBundle struct {
	key              string
	trxId            string
	effectiveBlockId uint64
	item             *quorumpb.ValidatorBundleItem
}

func (cs *Storage) UpdateProducerTrx(trx *quorumpb.Trx, acceptedBlockId uint64, prefix ...string) error {
	if trx == nil {
		return nil
	}
	item := &quorumpb.ValidatorBundleItem{}
	if err := proto.Unmarshal(trx.Data, item); err != nil {
		return err
	}
	if item.EffectiveBlockId == 0 || item.EffectiveBlockId <= acceptedBlockId {
		item.EffectiveBlockId = acceptedBlockId + 1
	}
	for _, producerItem := range item.Producers {
		if producerItem.GroupId == "" {
			producerItem.GroupId = trx.GroupId
		}
		producerItem.EffectiveBlockId = item.EffectiveBlockId
	}
	data, err := proto.Marshal(item)
	if err != nil {
		return err
	}
	key := s.GetPendingValidatorBundleKey(trx.GroupId, item.EffectiveBlockId, trx.TrxId, prefix...)
	return cs.dbmgr.Db.Set([]byte(key), data)
}

func (cs *Storage) saveProducerTrxId(groupId string, trxId string, prefix ...string) error {
	groupInfo, err := cs.GetGroupInfo(groupId)
	if err != nil {
		return err
	}

	key := s.GetProducerTrxIDKey(groupInfo.GroupId, prefix...)
	return cs.dbmgr.Db.Set([]byte(key), []byte(trxId))
}

func (cs *Storage) ApplyDueProducerUpdates(groupId string, nextBlockId uint64, prefix ...string) (int, error) {
	bundles, err := cs.getDueProducerBundles(groupId, nextBlockId, prefix...)
	if err != nil {
		return 0, err
	}
	sort.SliceStable(bundles, func(i, j int) bool {
		if bundles[i].effectiveBlockId == bundles[j].effectiveBlockId {
			return bundles[i].trxId < bundles[j].trxId
		}
		return bundles[i].effectiveBlockId < bundles[j].effectiveBlockId
	})
	for _, bundle := range bundles {
		if err := cs.UpdateProducerBundle(groupId, bundle.item, prefix...); err != nil {
			return 0, err
		}
		if err := cs.SetProducerSetVersion(groupId, bundle.effectiveBlockId, prefix...); err != nil {
			return 0, err
		}
		if err := cs.saveProducerTrxId(groupId, bundle.trxId, prefix...); err != nil {
			return 0, err
		}
		if err := cs.dbmgr.Db.Delete([]byte(bundle.key)); err != nil {
			return 0, err
		}
	}
	return len(bundles), nil
}

func (cs *Storage) getDueProducerBundles(groupId string, nextBlockId uint64, prefix ...string) ([]pendingValidatorBundle, error) {
	keyPrefix := s.GetPendingValidatorBundlePrefix(groupId, prefix...)
	bundles := []pendingValidatorBundle{}
	err := cs.dbmgr.Db.PrefixForeach([]byte(keyPrefix), func(k []byte, v []byte, err error) error {
		if err != nil {
			return err
		}
		item := &quorumpb.ValidatorBundleItem{}
		if err := proto.Unmarshal(v, item); err != nil {
			return err
		}
		if item.EffectiveBlockId > nextBlockId {
			return nil
		}
		key := string(append([]byte(nil), k...))
		bundles = append(bundles, pendingValidatorBundle{
			key:              key,
			trxId:            trxIdFromPendingProducerKey(key),
			effectiveBlockId: item.EffectiveBlockId,
			item:             item,
		})
		return nil
	})
	return bundles, err
}

func trxIdFromPendingProducerKey(key string) string {
	parts := strings.Split(key, "_")
	if len(parts) == 0 {
		return ""
	}
	return parts[len(parts)-1]
}

func (cs *Storage) GetUpdProducerListTrx(groupId string, prefix ...string) (*quorumpb.Trx, error) {
	key := s.GetProducerTrxIDKey(groupId, prefix...)
	btrx_id, err := cs.dbmgr.Db.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	trxId := string(btrx_id)

	trx, err := cs.GetTrx(groupId, trxId, def.Chain, prefix...)
	if err != nil {
		return nil, err
	}

	return trx, nil
}

func (cs *Storage) UpdateProducer(groupId string, data []byte, prefix ...string) error {
	item := &quorumpb.ValidatorBundleItem{}
	if err := proto.Unmarshal(data, item); err != nil {
		return err
	}
	return cs.UpdateProducerBundle(groupId, item, prefix...)
}

func (cs *Storage) UpdateProducerBundle(groupId string, item *quorumpb.ValidatorBundleItem, prefix ...string) error {
	groupInfo, err := cs.GetGroupInfo(groupId)
	if err != nil {
		return err
	}

	//Get all current producers (except owner)
	var cplist []string
	key := s.GetProducerPrefix(groupId, prefix...)
	err = cs.dbmgr.Db.PrefixForeach([]byte(key), func(k []byte, v []byte, err error) error {
		if err != nil {
			return err
		}
		item := &quorumpb.ProducerItem{}
		perr := proto.Unmarshal(v, item)
		if perr != nil {
			return perr
		}

		if item.ProducerPubkey != groupInfo.OwnerPubKey {
			pk, _ := localcrypto.Libp2pPubkeyToEthBase64(item.ProducerPubkey)
			if pk == "" {
				pk = item.ProducerPubkey
			}
			pkey := s.GetProducerKey(groupId, pk, prefix...)
			cplist = append(cplist, pkey)
		}

		return nil
	})

	if err != nil {
		return err
	}

	//remove all producers (except owner)
	for _, pkey := range cplist {
		err := cs.dbmgr.Db.Delete([]byte(pkey))
		if err != nil {
			return err
		}
	}

	//update with new producers list
	for _, producerItem := range item.Producers {
		pk, _ := localcrypto.Libp2pPubkeyToEthBase64(producerItem.ProducerPubkey)
		if pk == "" {
			pk = producerItem.ProducerPubkey
		}

		pdata, err := proto.Marshal(producerItem)
		if err != nil {
			return err
		}

		key := s.GetProducerKey(producerItem.GroupId, pk, prefix...)
		err = cs.dbmgr.Db.Set([]byte(key), pdata)
		if err != nil {
			return err
		}
	}

	return nil
}

func (cs *Storage) SetProducerSetVersion(groupId string, version uint64, prefix ...string) error {
	return cs.dbmgr.Db.Set([]byte(s.GetProducerSetVersionKey(groupId, prefix...)), []byte(strconv.FormatUint(version, 10)))
}

func (cs *Storage) GetProducerSetVersion(groupId string, prefix ...string) (uint64, error) {
	data, err := cs.dbmgr.Db.Get([]byte(s.GetProducerSetVersionKey(groupId, prefix...)))
	if err != nil || len(data) == 0 {
		return 0, err
	}
	return strconv.ParseUint(string(data), 10, 64)
}

func (cs *Storage) GetAllProducerInBytes(groupId string, Prefix ...string) ([][]byte, error) {
	key := s.GetProducerPrefix(groupId, Prefix...)
	var producerByteList [][]byte

	err := cs.dbmgr.Db.PrefixForeach([]byte(key), func(k []byte, v []byte, err error) error {
		if err != nil {
			return err
		}
		producerByteList = append(producerByteList, v)
		return nil
	})

	return producerByteList, err
}

func (cs *Storage) AddProducer(item *quorumpb.ProducerItem, prefix ...string) error {
	pk, _ := localcrypto.Libp2pPubkeyToEthBase64(item.ProducerPubkey)
	if pk == "" {
		pk = item.ProducerPubkey
	}

	key := s.GetProducerKey(item.GroupId, pk, prefix...)
	chaindb_log.Infof("Add Producer with key %s", key)

	pbyte, err := proto.Marshal(item)
	if err != nil {
		return err
	}
	return cs.dbmgr.Db.Set([]byte(key), pbyte)
}

func (cs *Storage) GetAnnouncedProducer(groupId string, pubkey string, prefix ...string) (*quorumpb.AnnounceItem, error) {
	key := s.GetAnnounceAsProducerKey(groupId, pubkey, prefix...)

	value, err := cs.dbmgr.Db.Get([]byte(key))
	if err != nil {
		return nil, err
	}

	var ann quorumpb.AnnounceItem
	err = proto.Unmarshal(value, &ann)
	if err != nil {
		return nil, err
	}

	return &ann, err
}

func (cs *Storage) IsProducerAnnounced(groupId, pubkey string, prefix ...string) (bool, error) {
	key := s.GetAnnounceAsProducerKey(groupId, pubkey, prefix...)
	return cs.dbmgr.Db.IsExist([]byte(key))
}
