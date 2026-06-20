package chainstorage

import (
	"encoding/hex"
	"errors"

	rumerrors "github.com/rumsystem/quorum/internal/pkg/errors"
	s "github.com/rumsystem/quorum/internal/pkg/storage"
	quorumpb "github.com/rumsystem/quorum/pkg/pb"
	"google.golang.org/protobuf/proto"
)

// add block
func (cs *Storage) AddBlock(block *quorumpb.Block, cached bool, prefix ...string) error {
	return cs.dbmgr.SaveBlock(block, cached, prefix...)
}

// add genesis block
func (cs *Storage) AddGensisBlock(block *quorumpb.Block, cached bool, prefix ...string) error {
	err := cs.dbmgr.SaveBlock(block, cached, prefix...)
	if err == rumerrors.ErrBlockExist {
		return nil
	}
	return err
}

// remove block
func (cs *Storage) RmBlock(groupId string, blockId uint64, cached bool, prefix ...string) error {
	return cs.dbmgr.RmBlock(groupId, blockId, cached, prefix...)
}

// get block by block_id
func (cs *Storage) GetBlock(groupId string, blockId uint64, cached bool, prefix ...string) (*quorumpb.Block, error) {
	return cs.dbmgr.GetBlock(groupId, blockId, cached, prefix...)
}

// check if block exist
func (cs *Storage) IsBlockExist(groupId string, blockId uint64, cached bool, prefix ...string) (bool, error) {
	return cs.dbmgr.IsBlockExist(groupId, blockId, cached, prefix...)
}

func (cs *Storage) GatherBlocksFromCache(block *quorumpb.Block, prefix ...string) ([]*quorumpb.Block, error) {
	var blocks []*quorumpb.Block
	blocks = append(blocks, block)
	currBlockId := block.BlockId
	pre := s.GetCachedBlockPrefix(block.GroupId, prefix...)
	err := cs.dbmgr.Db.PrefixForeach([]byte(pre), func(k []byte, v []byte, err error) error {
		if err != nil {
			return err
		}

		b := &quorumpb.Block{}
		perr := proto.Unmarshal(v, b)
		if perr != nil {
			return perr
		}

		currBlockId += 1
		if b.GroupId == block.GroupId && b.BlockId == currBlockId {
			blocks = append(blocks, b)
			return nil
		} else {
			return errors.New("NO_MORE_BLOCK")
		}
	})

	//search done, no more block to attach
	if err == nil || err.Error() == "NO_MORE_BLOCK" {
		return blocks, nil
	}

	return nil, err
}

func (cs *Storage) SaveSnowmanBlock(block *quorumpb.Block, status string, prefix ...string) error {
	if block == nil || len(block.BlockHash) == 0 {
		return errors.New("snowman block hash is empty")
	}
	blockHash := hex.EncodeToString(block.BlockHash)
	data, err := proto.Marshal(block)
	if err != nil {
		return err
	}
	if err := cs.dbmgr.Db.Set([]byte(s.GetSnowmanBlockKey(block.GroupId, blockHash, prefix...)), data); err != nil {
		return err
	}
	return cs.SetSnowmanBlockStatus(block.GroupId, blockHash, status, prefix...)
}

func (cs *Storage) GetSnowmanBlockByHash(groupId string, blockHash string, prefix ...string) (*quorumpb.Block, error) {
	data, err := cs.dbmgr.Db.Get([]byte(s.GetSnowmanBlockKey(groupId, blockHash, prefix...)))
	if err != nil {
		return nil, err
	}
	block := &quorumpb.Block{}
	if err := proto.Unmarshal(data, block); err != nil {
		return nil, err
	}
	return block, nil
}

func (cs *Storage) SetSnowmanBlockStatus(groupId string, blockHash string, status string, prefix ...string) error {
	return cs.dbmgr.Db.Set([]byte(s.GetSnowmanBlockStatusKey(groupId, blockHash, prefix...)), []byte(status))
}

func (cs *Storage) GetSnowmanBlockStatus(groupId string, blockHash string, prefix ...string) (string, error) {
	data, err := cs.dbmgr.Db.Get([]byte(s.GetSnowmanBlockStatusKey(groupId, blockHash, prefix...)))
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (cs *Storage) SetSnowmanLastAccepted(groupId string, blockHash string, prefix ...string) error {
	return cs.dbmgr.Db.Set([]byte(s.GetSnowmanLastAcceptedKey(groupId, prefix...)), []byte(blockHash))
}

func (cs *Storage) GetSnowmanLastAccepted(groupId string, prefix ...string) (string, error) {
	data, err := cs.dbmgr.Db.Get([]byte(s.GetSnowmanLastAcceptedKey(groupId, prefix...)))
	if err != nil {
		return "", err
	}
	return string(data), nil
}
