using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Holds the reduced fingerprint to the labels it is meant to forget.</summary>
/// <remarks>
/// The reduction merges states that differ only by renaming the members, so the reduced fingerprint has to be a function of
/// the state and not of the labels. It was not: when two members tied on every field the ordering key compares, the tie was
/// broken by id, and the vote masks and message endpoints printed against those ranks then differed between two states that
/// are one state under renaming. The exploration stopped merging them, and a comparison between the reduced and the
/// unreduced reachable sets reported a gap that belonged to the tie-break.
/// </remarks>
public sealed class ProtocolSymmetryTests
{
    private static readonly int[][] Permutations =
    [
        [0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0],
    ];

    /// <summary>Unique ordering keys keep their identity order with empty, small, and heap-fallback scratch buffers.</summary>
    /// <param name="count">The number of members.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(64)]
    [Arguments(65)]
    public async Task CanonicalScratchKeepsIdentityOrder(int count)
    {
        var nodes = new NodeState[count];
        for (var i = 0; i < nodes.Length; i++)
            nodes[i] = new NodeState(i, NodeRole.Follower, i + 1, -1, [], in NodeRuntime.Initial, false);

        var state = ClusterState.CreateInitial(count).WithNodes(nodes);
        _ = await Assert.That(state.Fingerprint(true)).IsEqualTo(state.Fingerprint(false));
    }

    /// <summary>A state and the same state with its members renamed must share one reduced fingerprint.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>
    /// The second reading is the control. The members are relabelled, so the unreduced fingerprints must differ — a state
    /// whose labels can be permuted without changing even those is a state where the test could not have failed. Zero
    /// labelled pairs would mean the control is dead rather than the key being right.
    /// </remarks>
    [Test]
    public async Task ReducedKeyIgnoresMemberLabels()
    {
        var states = ExploreRunner.ReachableStates(ExploreProfile.SmallTransfer(false), BrokenMode.None);
        var differing = 0;
        var split = 0;
        string? witness = null;
        for (var i = 0; i < states.Count; i++)
        {
            var state = states[i];
            var key = state.Fingerprint(true);
            var raw = state.Fingerprint(false);
            for (var p = 0; p < Permutations.Length; p++)
            {
                var renamed = Rename(state, Permutations[p]);
                if (!string.Equals(renamed.Fingerprint(false), raw, StringComparison.Ordinal))
                    differing++;

                if (string.Equals(renamed.Fingerprint(true), key, StringComparison.Ordinal))
                    continue;

                split++;
                witness ??= "key=" + key + " renamed=" + renamed.Fingerprint(true);
            }
        }

        _ = await Assert.That(differing > 0).IsTrue().Because($"the control is dead: no renaming changed an unreduced fingerprint in {states.Count} states");
        _ = await Assert.That(split).IsEqualTo(0).Because($"{split} renaming of {states.Count} states changed the reduced fingerprint; first {witness}");
    }

    /// <summary>The reduced run must reach every state the unreduced run reaches, up to renaming, and nothing else.</summary>
    /// <returns>The assertion task.</returns>
    /// <remarks>A merged state stands for every state it stands for, so a missing key is a set of transitions the
    /// reduced run never took and an extra key is a merge of states that are not one state under renaming.</remarks>
    [Test]
    public async Task ReducedRunReachesEveryOrbit()
    {
        var reduced = ExploreRunner.ReachableStates(ExploreProfile.SmallTransfer(), BrokenMode.None);
        var unreduced = ExploreRunner.ReachableStates(ExploreProfile.SmallTransfer(false), BrokenMode.None);
        var orbits = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < unreduced.Count; i++)
            _ = orbits.Add(unreduced[i].Fingerprint(true));

        var reached = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < reduced.Count; i++)
            _ = reached.Add(reduced[i].Fingerprint(true));

        var missing = 0;
        foreach (var orbit in orbits)
        {
            if (!reached.Contains(orbit))
                missing++;
        }

        var extra = 0;
        foreach (var key in reached)
        {
            if (!orbits.Contains(key))
                extra++;
        }

        _ = await Assert.That(orbits.Count).IsGreaterThan(0).Because("the unreduced run reached nothing");
        _ = await Assert.That(missing).IsEqualTo(0).Because($"{missing} of {orbits.Count} orbits are unreachable in the reduced run");
        _ = await Assert.That(extra).IsEqualTo(0).Because($"{extra} reduced keys are not states of the unreduced run");
        _ = await Assert.That(reached.Count).IsEqualTo(orbits.Count)
            .Because($"reduced {reached.Count.ToString(CultureInfo.InvariantCulture)} vs orbits {orbits.Count.ToString(CultureInfo.InvariantCulture)}");
    }

    private static ClusterState Rename(ClusterState state, int[] permutation)
    {
        var count = state.Nodes.Count;
        var nodes = new NodeState[count];
        var partitions = new int[count];
        var matchIndexes = new int[count];
        for (var i = 0; i < count; i++)
        {
            var node = state.Nodes[i];
            var id = permutation[i];
            var votedFor = node.VotedFor < 0 ? -1 : permutation[node.VotedFor];
            var runtime = NodeRuntime.Create(
                node.CommitIndex,
                node.AppliedIndex,
                VoteMask.Remap(node.VotesGranted, permutation),
                node.ReadIndex,
                VoteMask.Remap(node.ReadAcks, permutation),
                node.ReadReady,
                node.Stopping) with
            {
                KnownLeaderTerm = node.KnownLeaderTerm,
                KnownLeaderId = node.KnownLeaderId < 0 ? -1 : permutation[node.KnownLeaderId],
            };
            nodes[id] = new NodeState(id, node.Role, node.CurrentTerm, votedFor, node.LogEntries, in runtime, node.Unseeded);
            partitions[id] = state.Partitions[i];
            matchIndexes[id] = state.MatchIndexes[i];
        }

        var messages = new List<InFlightMessage>(state.Messages.Count);
        for (var i = 0; i < state.Messages.Count; i++)
        {
            var m = state.Messages[i];
            messages.Add(new InFlightMessage(m.Id, m.Kind, permutation[m.From], permutation[m.To], m.Term, m.LastLogIndex, m.LastLogTerm, m.Success, m.MatchIndex, m.ReadIndex, m.PrevLogTerm));
        }

        var elections = new ModelElectionRecord[state.ElectedLeaders.Count];
        for (var i = 0; i < state.ElectedLeaders.Count; i++)
        {
            var record = state.ElectedLeaders[i];
            elections[i] = new ModelElectionRecord(record.Term, permutation[record.NodeId]);
        }

        return state.WithNodesMessages(nodes, messages, state.NextMessageId)
            .WithPartitions(partitions)
            .WithMatchIndexes(matchIndexes)
            .With(elections: elections);
    }
}
