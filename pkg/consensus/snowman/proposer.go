package snowman

import (
	"crypto/sha256"
	"encoding/binary"
	"sort"
	"time"
)

type ProposerWindow struct {
	Validator string
	Index     int
	Open      bool
}

func ProposerFor(parentHash []byte, height uint64, producerSetVersion uint64, validators []string, now time.Time, params Params) ProposerWindow {
	if len(validators) == 0 {
		return ProposerWindow{Open: true}
	}
	order := proposerOrder(parentHash, height, producerSetVersion, validators)
	window := int(now.UnixMilli()/int64(params.ProposerWindowMs)) % (len(order) + 1)
	if params.ProposerWindowCount > 0 && params.ProposerWindowCount < len(order) {
		window = int(now.UnixMilli()/int64(params.ProposerWindowMs)) % (params.ProposerWindowCount + 1)
	}
	if window >= len(order) {
		return ProposerWindow{Open: true, Index: window}
	}
	return ProposerWindow{Validator: order[window], Index: window}
}

func proposerOrder(parentHash []byte, height uint64, producerSetVersion uint64, validators []string) []string {
	order := make([]string, 0, len(validators))
	order = append(order, validators...)
	sort.Slice(order, func(i, j int) bool {
		return proposerScore(parentHash, height, producerSetVersion, order[i]) < proposerScore(parentHash, height, producerSetVersion, order[j])
	})
	return order
}

func proposerScore(parentHash []byte, height uint64, producerSetVersion uint64, validator string) uint64 {
	h := sha256.New()
	h.Write(parentHash)
	var b [16]byte
	binary.BigEndian.PutUint64(b[:8], height)
	binary.BigEndian.PutUint64(b[8:], producerSetVersion)
	h.Write(b[:])
	h.Write([]byte(validator))
	sum := h.Sum(nil)
	return binary.BigEndian.Uint64(sum[:8])
}
