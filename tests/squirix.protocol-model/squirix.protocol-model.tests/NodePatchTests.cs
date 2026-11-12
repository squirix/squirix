using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Optional overrides preserve omitted values and distinguish explicit zero, false and minus one.</summary>
public sealed class NodePatchTests
{
    /// <summary>Every stored field survives an omitted override and can be reset explicitly.</summary>
    /// <param name="reset">Whether every field is explicitly reset.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OverridesDistinguishOmittedValues(bool reset)
    {
        var runtime = NodeRuntime.Create(1, 1, 3, 1, 3, true, true) with { KnownLeaderTerm = 4, KnownLeaderId = 2 };
        var original = new NodeState(0, NodeRole.Leader, 4, 2, [new LogEntry(4, 1)], in runtime, true);
        var patch = reset
            ? new NodePatch
            {
                AppliedIndex = 0,
                CommitIndex = 0,
                CurrentTerm = 0,
                LogEntries = [],
                ReadAcks = 0,
                ReadIndex = 0,
                ReadReady = false,
                Role = NodeRole.Follower,
                VotedFor = -1,
                VotesGranted = 0,
                Stopping = false,
                KnownLeaderTerm = 0,
                KnownLeaderId = -1,
                Unseeded = false,
            }
            : default;
        var actual = ModelTransitionUtil.Patch(original, in patch);
        var expected = reset ? NodeState.CreateInitial(0) : original;
        var actualState = ClusterState.CreateInitial(3).WithNodes([actual, NodeState.CreateInitial(1), NodeState.CreateInitial(2)]);
        var expectedState = ClusterState.CreateInitial(3).WithNodes([expected, NodeState.CreateInitial(1), NodeState.CreateInitial(2)]);

        _ = await Assert.That(actualState.Fingerprint(false)).IsEqualTo(expectedState.Fingerprint(false));
    }
}
