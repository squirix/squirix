using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class ReachabilityOrderTests
{
    /// <summary>The list-backed traversal retains the exact order of a queue-based breadth-first traversal.</summary>
    /// <param name="symmetryReduce">Whether equivalent member labellings share a visited key.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReachableStatesKeepBreadthFirstOrder(bool symmetryReduce)
    {
        var profile = ExploreProfile.ForReplicaCount(2, 1, 0, 2, 0, false, symmetryReduce);
        var initial = ClusterState.CreateInitial(2);
        var queue = new Queue<ClusterState>();
        queue.Enqueue(initial);
        var seen = new HashSet<string>(StringComparer.Ordinal) { initial.Fingerprint(symmetryReduce) };
        var expected = new List<string>();
        while (queue.Count > 0)
        {
            var current = queue.Dequeue();
            expected.Add(current.Fingerprint(symmetryReduce));
            var successors = ExploreRunner.NextStates(current, profile, BrokenMode.None);
            for (var i = 0; i < successors.Count; i++)
            {
                if (seen.Add(successors[i].Fingerprint(symmetryReduce)))
                    queue.Enqueue(successors[i]);
            }
        }

        var actual = ExploreRunner.ReachableStates(profile, BrokenMode.None);
        _ = await Assert.That(expected.Count).IsGreaterThan(3);
        _ = await Assert.That(actual.Count).IsEqualTo(expected.Count);
        for (var i = 0; i < actual.Count; i++)
            _ = await Assert.That(actual[i].Fingerprint(symmetryReduce)).IsEqualTo(expected[i]);
    }
}
