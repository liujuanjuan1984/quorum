package snowman

import "fmt"

type Params struct {
	K                    int
	AlphaPreference      int
	AlphaConfidence      int
	BetaVirtuous         int
	BetaRogue            int
	ConcurrentRepolls    int
	OptimalProcessing    int
	MaxOutstandingPolls  int
	RoundTimeoutMs       int
	ProposerWindowMs     int
	ProposerWindowCount  int
	MaxBlockTransactions int
}

func DefaultParams(validators int) Params {
	k := validators
	if k > 20 {
		k = 20
	}
	if k < 1 {
		k = 1
	}
	alpha := (k / 2) + 1
	return Params{
		K:                    k,
		AlphaPreference:      alpha,
		AlphaConfidence:      alpha,
		BetaVirtuous:         8,
		BetaRogue:            12,
		ConcurrentRepolls:    4,
		OptimalProcessing:    32,
		MaxOutstandingPolls:  128,
		RoundTimeoutMs:       1500,
		ProposerWindowMs:     1000,
		ProposerWindowCount:  validators,
		MaxBlockTransactions: 20,
	}
}

func (p Params) Validate(validators int) error {
	if validators <= 0 {
		return fmt.Errorf("snowman requires at least one validator")
	}
	if p.K <= 0 || p.K > validators {
		return fmt.Errorf("invalid K %d for %d validators", p.K, validators)
	}
	if p.AlphaPreference <= p.K/2 || p.AlphaPreference > p.K {
		return fmt.Errorf("invalid alpha preference %d for K %d", p.AlphaPreference, p.K)
	}
	if p.AlphaConfidence <= p.K/2 || p.AlphaConfidence > p.K {
		return fmt.Errorf("invalid alpha confidence %d for K %d", p.AlphaConfidence, p.K)
	}
	if p.BetaVirtuous <= 0 || p.BetaRogue <= 0 {
		return fmt.Errorf("beta thresholds must be positive")
	}
	if p.BetaRogue < p.BetaVirtuous {
		return fmt.Errorf("rogue beta must be greater than or equal to virtuous beta")
	}
	if p.ProposerWindowMs <= 0 {
		return fmt.Errorf("proposer window must be positive")
	}
	if p.MaxBlockTransactions <= 0 {
		return fmt.Errorf("max block transactions must be positive")
	}
	return nil
}
