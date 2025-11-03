package integration

import (
	"fmt"
	"sync"

	"github.com/onflow/flow-go/consensus/hotstuff"
	"github.com/onflow/flow-go/consensus/hotstuff/committees"
	"github.com/onflow/flow-go/model/flow"
	"github.com/onflow/flow-go/state/protocol"
)

// TwinsConsensusCommittee allows specifying a custom leader for each view.
// It wraps an existing committee and overrides only the LeaderForView method.
// It implements both hotstuff.DynamicCommittee and component lifecycle interfaces.
type TwinsConsensusCommittee struct {
	*committees.Consensus
	mu                      sync.RWMutex
	leadersByView           map[uint64]flow.Identifier              // explicit leader mapping
	quorumThresholdOverride func(view uint64) (uint64, bool, error) // optional quorum threshold override
}

// NewTwinsConsensusCommittee creates a custom leader selection by wrapping a consensus committee.
// The returned committee can be used anywhere a *committees.Consensus is expected,
// but will use the custom leader selection strategy for determining leaders.
// If no explicit leader is set for a view, it uses the provided strategy.
// quorumThresholdOverride is optional (can be nil) and allows overriding quorum threshold calculation.
func NewTwinsConsensusCommittee(
	state protocol.State,
	me flow.Identifier,
	leadersByView map[uint64]flow.Identifier,
	quorumThresholdOverride func(view uint64) (uint64, bool, error),
) (*TwinsConsensusCommittee, error) {
	// Create the underlying consensus committee
	consensus, err := committees.NewConsensusCommittee(state, me)
	if err != nil {
		return nil, fmt.Errorf("could not create consensus committee: %w", err)
	}

	return &TwinsConsensusCommittee{
		Consensus:               consensus,
		leadersByView:           leadersByView,
		quorumThresholdOverride: quorumThresholdOverride,
	}, nil
}

// LeaderForView returns the leader for the given view. If a custom leader is set for this view,
// it returns that. Otherwise, it delegates to the default strategy.
func (c *TwinsConsensusCommittee) LeaderForView(view uint64) (flow.Identifier, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if leaderID, ok := c.leadersByView[view]; ok {
		return leaderID, nil
	} else {
		return flow.ZeroID, fmt.Errorf("leader not found for view %d", view)
	}
}

// Replicas interface implementation - proxy to embedded Consensus

func (c *TwinsConsensusCommittee) QuorumThresholdForView(view uint64) (uint64, error) {
	c.mu.RLock()
	override := c.quorumThresholdOverride
	c.mu.RUnlock()

	if override != nil {
		threshold, useOverride, err := override(view)
		if err != nil {
			return 0, err
		}
		if useOverride {
			return threshold, nil
		}
	}

	return c.Consensus.QuorumThresholdForView(view)
}

func (c *TwinsConsensusCommittee) TimeoutThresholdForView(view uint64) (uint64, error) {
	return c.Consensus.TimeoutThresholdForView(view)
}

func (c *TwinsConsensusCommittee) Self() flow.Identifier {
	return c.Consensus.Self()
}

func (c *TwinsConsensusCommittee) DKG(view uint64) (hotstuff.DKG, error) {
	return c.Consensus.DKG(view)
}

func (c *TwinsConsensusCommittee) IdentitiesByEpoch(view uint64) (flow.IdentitySkeletonList, error) {
	return c.Consensus.IdentitiesByEpoch(view)
}

func (c *TwinsConsensusCommittee) IdentityByEpoch(view uint64, participantID flow.Identifier) (*flow.IdentitySkeleton, error) {
	return c.Consensus.IdentityByEpoch(view, participantID)
}

// DynamicCommittee interface implementation - proxy to embedded Consensus

func (c *TwinsConsensusCommittee) IdentitiesByBlock(blockID flow.Identifier) (flow.IdentityList, error) {
	return c.Consensus.IdentitiesByBlock(blockID)
}

func (c *TwinsConsensusCommittee) IdentityByBlock(blockID flow.Identifier, participantID flow.Identifier) (*flow.Identity, error) {
	return c.Consensus.IdentityByBlock(blockID, participantID)
}

// protocol.Consumer interface implementation - proxy to embedded Consensus

func (c *TwinsConsensusCommittee) BlockFinalized(block *flow.Header) {
	c.Consensus.BlockFinalized(block)
}

func (c *TwinsConsensusCommittee) BlockProcessable(block *flow.Header, certifyingQC *flow.QuorumCertificate) {
	c.Consensus.BlockProcessable(block, certifyingQC)
}

func (c *TwinsConsensusCommittee) EpochTransition(newEpochCounter uint64, first *flow.Header) {
	c.Consensus.EpochTransition(newEpochCounter, first)
}

func (c *TwinsConsensusCommittee) EpochSetupPhaseStarted(currentEpochCounter uint64, first *flow.Header) {
	c.Consensus.EpochSetupPhaseStarted(currentEpochCounter, first)
}

func (c *TwinsConsensusCommittee) EpochCommittedPhaseStarted(currentEpochCounter uint64, first *flow.Header) {
	c.Consensus.EpochCommittedPhaseStarted(currentEpochCounter, first)
}

func (c *TwinsConsensusCommittee) EpochFallbackModeTriggered(epochCounter uint64, header *flow.Header) {
	c.Consensus.EpochFallbackModeTriggered(epochCounter, header)
}

func (c *TwinsConsensusCommittee) EpochFallbackModeExited(epochCounter uint64, header *flow.Header) {
	c.Consensus.EpochFallbackModeExited(epochCounter, header)
}

func (c *TwinsConsensusCommittee) EpochExtended(epochCounter uint64, header *flow.Header, extension flow.EpochExtension) {
	c.Consensus.EpochExtended(epochCounter, header, extension)
}

// Verify that TwinsConsensus implements the required interfaces
var _ hotstuff.DynamicCommittee = (*TwinsConsensusCommittee)(nil)
var _ hotstuff.Replicas = (*TwinsConsensusCommittee)(nil)
var _ protocol.Consumer = (*TwinsConsensusCommittee)(nil)
