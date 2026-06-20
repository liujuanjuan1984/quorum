package snowman

import (
	"fmt"
	"time"
)

type Poll struct {
	RequestID string
	Options   []string
	Sampled   map[string]bool
	Votes     map[string]string
	Deadline  time.Time
}

func NewPoll(requestID string, sampled []string, options []string, timeout time.Duration) *Poll {
	p := &Poll{
		RequestID: requestID,
		Options:   append([]string(nil), options...),
		Sampled:   make(map[string]bool, len(sampled)),
		Votes:     make(map[string]string, len(sampled)),
		Deadline:  time.Now().Add(timeout),
	}
	for _, validator := range sampled {
		p.Sampled[validator] = true
	}
	return p
}

func (p *Poll) AddVote(validator, preference string) error {
	if p == nil {
		return fmt.Errorf("poll is nil")
	}
	if !p.Sampled[validator] {
		return fmt.Errorf("validator %s was not sampled for poll %s", validator, p.RequestID)
	}
	if _, ok := p.Votes[validator]; ok {
		return fmt.Errorf("validator %s already voted for poll %s", validator, p.RequestID)
	}
	if !p.hasOption(preference) {
		return fmt.Errorf("preference %s is not an option for poll %s", preference, p.RequestID)
	}
	p.Votes[validator] = preference
	return nil
}

func (p *Poll) hasOption(preference string) bool {
	for _, option := range p.Options {
		if option == preference {
			return true
		}
	}
	return false
}

func (p *Poll) Finished() bool {
	if p == nil {
		return false
	}
	return len(p.Votes) == len(p.Sampled) || time.Now().After(p.Deadline)
}

func (p *Poll) Result() (string, int) {
	counts := map[string]int{}
	for _, preference := range p.Votes {
		counts[preference]++
	}
	var best string
	var bestCount int
	for preference, count := range counts {
		if count > bestCount || best == "" {
			best = preference
			bestCount = count
		}
	}
	return best, bestCount
}
