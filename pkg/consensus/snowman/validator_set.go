package snowman

import (
	"crypto/rand"
	"encoding/binary"
	"sort"
)

type ValidatorSet struct {
	nodes []string
	index map[string]int
}

func NewValidatorSet(nodes []string) *ValidatorSet {
	uniq := make(map[string]struct{}, len(nodes))
	for _, node := range nodes {
		if node != "" {
			uniq[node] = struct{}{}
		}
	}
	sorted := make([]string, 0, len(uniq))
	for node := range uniq {
		sorted = append(sorted, node)
	}
	sort.Strings(sorted)
	index := make(map[string]int, len(sorted))
	for i, node := range sorted {
		index[node] = i
	}
	return &ValidatorSet{nodes: sorted, index: index}
}

func (v *ValidatorSet) Len() int {
	if v == nil {
		return 0
	}
	return len(v.nodes)
}

func (v *ValidatorSet) Nodes() []string {
	if v == nil {
		return nil
	}
	out := make([]string, len(v.nodes))
	copy(out, v.nodes)
	return out
}

func (v *ValidatorSet) Contains(pubkey string) bool {
	if v == nil {
		return false
	}
	_, ok := v.index[pubkey]
	return ok
}

func (v *ValidatorSet) Sample(k int) []string {
	if v == nil || len(v.nodes) == 0 || k <= 0 {
		return nil
	}
	nodes := v.Nodes()
	shuffle(nodes)
	if k > len(nodes) {
		k = len(nodes)
	}
	return nodes[:k]
}

func shuffle(nodes []string) {
	for i := len(nodes) - 1; i > 0; i-- {
		j := int(randUint64() % uint64(i+1))
		nodes[i], nodes[j] = nodes[j], nodes[i]
	}
}

func randUint64() uint64 {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return 0
	}
	return binary.LittleEndian.Uint64(b[:])
}
