using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The fault schedule repeats for a seed, never touches its anchor, and keeps a majority of the replicas alive and connected.</summary>
public sealed class FaultScheduleModelTests : EndToEndTestBase
{
    /// <summary>Two schedules with the same seed hand out the same steps, and another seed gives other steps.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task SameSeedGivesSameSteps()
    {
        var first = Run(FailoverSteps.FiveNodes, 5, 1234UL, 200);
        var second = Run(FailoverSteps.FiveNodes, 5, 1234UL, 200);
        var other = Run(FailoverSteps.FiveNodes, 5, 1235UL, 200);

        _ = await Assert.That(Same(first, second)).IsTrue();
        _ = await Assert.That(Same(first, other)).IsFalse();
    }

    /// <summary>Whatever the seed, no step touches the anchor and at most a minority of the replicas is stopped or isolated at once.</summary>
    /// <param name="replicas">The replica factor, which is also the number of nodes.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(3)]
    [Arguments(5)]
    public async Task ScheduleKeepsMajorityAndAnchor(int replicas)
    {
        var nodes = replicas == 3 ? FailoverSteps.ThreeNodes : FailoverSteps.FiveNodes;
        var anchor = nodes[^1];
        var minority = (replicas - 1) / 2;
        var violations = new List<string>();
        for (var seed = 0UL; seed < 50UL; seed++)
        {
            var schedule = new FaultSchedule(nodes, anchor, replicas, seed);
            for (var i = 0; i < 100; i++)
            {
                var step = schedule.Next(i % 2 == 0 ? nodes[0] : string.Empty);
                var kept = schedule.Healthy().Length >= replicas - minority && schedule.Running().Length >= replicas - minority && Array.IndexOf(schedule.Healthy(), anchor) >= 0;
                if (string.Equals(step.NodeId, anchor, StringComparison.Ordinal) || !kept)
                    violations.Add($"seed {seed} step {i}: {step}");
            }
        }

        _ = await Assert.That(violations).IsEmpty();
    }

    private static bool Same(List<FaultStep> left, List<FaultStep> right)
    {
        if (left.Count != right.Count)
            return false;

        for (var i = 0; i < left.Count; i++)
        {
            if (left[i] != right[i])
                return false;
        }

        return true;
    }

    private static List<FaultStep> Run(string[] nodes, int replicas, ulong seed, int count)
    {
        var schedule = new FaultSchedule(nodes, nodes[^1], replicas, seed);
        var steps = new List<FaultStep>(count);
        for (var i = 0; i < count; i++)
            steps.Add(schedule.Next(nodes[i % nodes.Length]));

        return steps;
    }
}
