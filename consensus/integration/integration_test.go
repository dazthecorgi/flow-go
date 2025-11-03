package integration_test

import (
	"context"
	"fmt"
	"sort"

	// "sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/onflow/flow-go/consensus/integration"
	"github.com/onflow/flow-go/model/flow"

	// "github.com/onflow/flow-go/model/messages"
	"github.com/onflow/flow-go/module/irrecoverable"
	"github.com/onflow/flow-go/module/util"
	"github.com/onflow/flow-go/network/channels"
	"github.com/onflow/flow-go/state/protocol"
	"github.com/onflow/flow-go/utils/unittest"
)

func runNodes(signalerCtx irrecoverable.SignalerContext, nodes []*Node) {
	for _, n := range nodes {
		go func(n *Node) {
			n.committee.Start(signalerCtx)
			n.hot.Start(signalerCtx)
			n.voteAggregator.Start(signalerCtx)
			n.timeoutAggregator.Start(signalerCtx)
			n.compliance.Start(signalerCtx)
			n.messageHub.Start(signalerCtx)
			n.sync.Start(signalerCtx)
			<-util.AllReady(n.committee, n.hot, n.voteAggregator, n.timeoutAggregator, n.compliance, n.sync, n.messageHub)
		}(n)
	}
}

func stopNodes(t *testing.T, cancel context.CancelFunc, nodes []*Node) {
	stoppingNodes := make([]<-chan struct{}, 0)
	cancel()
	for _, n := range nodes {
		stoppingNodes = append(stoppingNodes, util.AllDone(
			n.committee,
			n.hot,
			n.voteAggregator,
			n.timeoutAggregator,
			n.compliance,
			n.sync,
			n.messageHub,
		))
	}
	unittest.RequireCloseBefore(t, util.AllClosed(stoppingNodes...), time.Second, "requiring nodes to stop")
}

func TestTwinsScenario(t *testing.T) {
	numIdentities := 4
	participantsData := createConsensusIdentities(t, numIdentities)

	// TODO vary root snapshot so that twins have different starting states
	rootSnapshot := createRootSnapshot(t, participantsData)

	// Extract node identities for leader selection
	nodeIDs := make([]flow.Identifier, 0, len(participantsData.Participants))
	for _, participant := range participantsData.Participants {
		nodeIDs = append(nodeIDs, participant.NodeID)
	}

	// Create a custom leader selection factory
	// This maps each view to a specific leader from the participants
	customCommitteeFactory := func(state protocol.State, me flow.Identifier) (Committee, error) {
		leadersByView := make(map[uint64]flow.Identifier)
		for i := uint64(0); i < 1000; i++ { // Schedule leaders for first 10 views
			// Always the byzantine nodes as leader
			leadersByView[i] = nodeIDs[0]
		}

		// Override quorum threshold to 1
		quorumThresholdOverride := func(view uint64) (uint64, bool, error) {
			// Weight, not participants
			return 1000, true, nil
		}

		custom, err := integration.NewTwinsConsensusCommittee(state, me, leadersByView, quorumThresholdOverride)
		if err != nil {
			return nil, err
		}
		return custom, nil
	}

	stopper := NewStopper(5, 0)

	// First node is is duplicated for twins behavior
	nodes, hub, runFor := createNodesWithCommitteeFactory(t, NewConsensusParticipants(participantsData), rootSnapshot, stopper, customCommitteeFactory)
	require.Equal(t, numIdentities + 1, len(nodes))

	// print node ids
	for _, n := range nodes {
		fmt.Printf("Node ID: %s\n", n.id.NodeID)
	}

	// Create a filter that tracks view height for consensus messages and applies partitions
	partitionsFilter := func(channel channels.Channel, event interface{}, sender, receiver *Node) (bool, time.Duration) {
		block := false
		// First node which is byzantine, and the 2nd and 3rd nodes are one partition
		if (sender.id.NodeID == nodeIDs[0] && !sender.isTwin) && !((receiver.id.NodeID == nodeIDs[0] && !receiver.isTwin) || receiver.id.NodeID == nodeIDs[1] || receiver.id.NodeID == nodeIDs[2]) {
			block = true
		}
		if sender.id.NodeID == nodeIDs[1] && !((receiver.id.NodeID == nodeIDs[0] && !receiver.isTwin) || receiver.id.NodeID == nodeIDs[1] || receiver.id.NodeID == nodeIDs[2]) {
			block = true
		}
		if sender.id.NodeID == nodeIDs[2] && !((receiver.id.NodeID == nodeIDs[0] && !receiver.isTwin) || receiver.id.NodeID == nodeIDs[1] || receiver.id.NodeID == nodeIDs[2]) {
			block = true
		}

		// Twin byzantine node and the 4th node are another partition
		if sender.isTwin && !(receiver.isTwin || receiver.id.NodeID == nodeIDs[3]) {
			block = true
		}
		if sender.id.NodeID == nodeIDs[3] && !(receiver.isTwin || receiver.id.NodeID == nodeIDs[3]) {
			block = true
		}

		return block, 0
	}

	hub.WithFilter(partitionsFilter)

	runFor(30 * time.Second)

	allViews := allFinalizedViews(t, nodes)
	assertSafety(t, allViews)

	cleanupNodes(nodes)
}

// happy path: with 3 nodes, they can reach consensus
func Test3Nodes(t *testing.T) {
	stopper := NewStopper(5, 0)
	participantsData := createConsensusIdentities(t, 3)
	rootSnapshot := createRootSnapshot(t, participantsData)
	nodes, hub, runFor := createNodes(t, NewConsensusParticipants(participantsData), rootSnapshot, stopper)

	hub.WithFilter(blockNothing)

	runFor(30 * time.Second)

	allViews := allFinalizedViews(t, nodes)
	assertSafety(t, allViews)

	cleanupNodes(nodes)
}

// with 5 nodes, and one node completely blocked, the other 4 nodes can still reach consensus
func Test5Nodes(t *testing.T) {
	// 4 nodes should be able to finalize at least 3 blocks.
	stopper := NewStopper(2, 1)
	participantsData := createConsensusIdentities(t, 5)
	rootSnapshot := createRootSnapshot(t, participantsData)
	nodes, hub, runFor := createNodes(t, NewConsensusParticipants(participantsData), rootSnapshot, stopper)

	hub.WithFilter(blockNodes(nodes[0]))

	runFor(30 * time.Second)

	header, err := nodes[0].state.Final().Head()
	require.NoError(t, err)

	// the first node was blocked, never finalize any block
	require.Equal(t, uint64(0), header.View)

	allViews := allFinalizedViews(t, nodes[1:])
	assertSafety(t, allViews)

	cleanupNodes(nodes)
}

// TODO: verify if each receiver lost 50% messages, the network can't reach consensus

func allFinalizedViews(t *testing.T, nodes []*Node) [][]uint64 {
	allViews := make([][]uint64, 0)

	// verify all nodes arrive at the same state
	for _, node := range nodes {
		views := chainViews(t, node)
		fmt.Printf("chain length for node %x %t: %d\n", node.id.NodeID, node.isTwin, len(views))
		allViews = append(allViews, views)
	}

	// sort all Views by chain length
	sort.Slice(allViews, func(i, j int) bool {
		return len(allViews[i]) < len(allViews[j])
	})

	return allViews
}

func assertSafety(t *testing.T, allViews [][]uint64) {
	// find the longest chain of finalized views
	longest := allViews[len(allViews)-1]

	for _, views := range allViews {
		// each view in a chain should match with the longest chain
		for j, view := range views {
			require.Equal(t, longest[j], view, "each view in a chain must match with the view in longest chain at the same height, but didn't")
		}
	}
}

func chainViews(t *testing.T, node *Node) []uint64 {
	views := make([]uint64, 0)

	head, err := node.state.Final().Head()
	require.NoError(t, err)
	for head != nil && head.ContainsParentQC() {
		views = append(views, head.View)
		head, err = node.headers.ByBlockID(head.ParentID)
		require.NoError(t, err)
	}

	// reverse all views to runFor from lower view to higher view
	low2high := make([]uint64, 0)
	for i := len(views) - 1; i >= 0; i-- {
		low2high = append(low2high, views[i])
	}
	return low2high
}

// BlockOrDelayFunc is a function for deciding whether a message (or other event) should be
// blocked or delayed. The first return value specifies whether the event should be dropped
// entirely (return value `true`) or should be delivered (return value `false`). The second
// return value specifies the delay by which the message should be delivered.
// Implementations must be CONCURRENCY SAFE.
type BlockOrDelayFunc func(channel channels.Channel, event interface{}, sender, receiver *Node) (bool, time.Duration)

// blockNothing specifies that _all_ messages should be delivered without delay.
// I.e. this function returns always `false` (no blocking), `0` (no delay).
func blockNothing(_ channels.Channel, _ interface{}, _, _ *Node) (bool, time.Duration) {
	return false, 0
}

// blockNodes specifies that all messages sent or received by any member of the `denyList`
// should be dropped, i.e. we return `true` (block message), `0` (no delay).
// For nodes _not_ in the `denyList`,  we return `false` (no blocking), `0` (no delay).
func blockNodes(denyList ...*Node) BlockOrDelayFunc {
	denyMap := make(map[flow.Identifier]*Node, len(denyList))
	for _, n := range denyList {
		denyMap[n.id.NodeID] = n
	}
	// no concurrency protection needed as blackList is only read but not modified
	return func(channel channels.Channel, event interface{}, sender, receiver *Node) (bool, time.Duration) {
		if _, ok := denyMap[sender.id.NodeID]; ok {
			return true, 0 // block the message
		}
		if _, ok := denyMap[receiver.id.NodeID]; ok {
			return true, 0 // block the message
		}
		return false, 0 // allow the message
	}
}
