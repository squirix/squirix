using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>The fault schedule repeats for a seed, never touches its anchor, and keeps a majority of the replicas alive and connected.</summary>
public sealed class FaultScheduleModelTests : EndToEndTestBase
{
    /// <summary>Two schedules with the same seed and leaders hand out the same steps, and another seed gives other steps.</summary>
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

    /// <summary>
    /// Replaying the steps into separate sets of stopped and isolated nodes shows that no step touches the anchor, a fault strikes a running
    /// and connected node, a restart strikes a stopped one, a heal needs a cut link, at most a minority is down at once, a drain restores
    /// every node, and every kind of step occurs.
    /// </summary>
    /// <param name="replicas">The replica factor, which is also the number of nodes.</param>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    [Arguments(3)]
    [Arguments(5)]
    public async Task ScheduleKeepsMajorityAndAnchor(int replicas)
    {
        var nodes = replicas == 3 ? FailoverSteps.ThreeNodes : FailoverSteps.FiveNodes;
        var violations = new List<string>();
        var kinds = new HashSet<FaultKind>();
        var leaderFaults = 0;
        for (var seed = 0UL; seed < 20UL; seed++)
            leaderFaults += ReplaySeed(nodes, replicas, seed, kinds, violations);

        _ = await Assert.That(violations).IsEmpty();
        _ = await Assert.That(kinds.Count).IsEqualTo(5);
        _ = await Assert.That(leaderFaults).IsGreaterThan(0);
    }

    /// <summary>The first fault of a schedule strikes the leader whenever the leader is not the anchor.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task FirstFaultStrikesTheLeader()
    {
        var misses = 0;
        for (var seed = 0UL; seed < 50UL; seed++)
        {
            var step = new FaultSchedule(FailoverSteps.FiveNodes, "nodeE", 5, seed).Next("nodeB");
            misses += step.HitsLeader && string.Equals(step.NodeId, "nodeB", StringComparison.Ordinal) ? 0 : 1;
        }

        _ = await Assert.That(misses).IsEqualTo(0);
    }

    /// <summary>A schedule needs at least three replicas, where a minority can fail.</summary>
    /// <returns>A task that represents the asynchronous test.</returns>
    [Test]
    public async Task ScheduleRejectsFewerThanThreeReplicas()
    {
        var failure = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => _ = new FaultSchedule(["nodeA", "nodeB"], "nodeB", 2, 1UL));

        _ = await Assert.That(failure.ParamName).IsEqualTo("replicaCount");
    }

    private static int ReplaySeed(string[] nodes, int replicas, ulong seed, HashSet<FaultKind> kinds, List<string> violations)
    {
        var anchor = nodes[^1];
        var leaderFaults = 0;
        var schedule = new FaultSchedule(nodes, anchor, replicas, seed);
        var down = (Stopped: new HashSet<string>(StringComparer.Ordinal), Isolated: new HashSet<string>(StringComparer.Ordinal));
        for (var i = 0; i < 100; i++)
        {
            var leader = i % 3 == 0 ? string.Empty : nodes[i % (nodes.Length - 1)];
            var step = schedule.Next(leader);
            _ = kinds.Add(step.Kind);
            leaderFaults += step.HitsLeader ? 1 : 0;
            var problem = Replay(step, anchor, down, leader, (replicas - 1) / 2);
            if (problem == null && !AgreesOnNodesUp(schedule, nodes, down))
                problem = "the schedule disagrees about which nodes are up";

            if (problem != null)
                violations.Add($"seed {seed} step {i}: {step}: {problem}");
        }

        AssertDrained(schedule, nodes, down, seed, violations);
        return leaderFaults;
    }

    private static void AssertDrained(FaultSchedule schedule, string[] nodes, (HashSet<string> Stopped, HashSet<string> Isolated) down, ulong seed, List<string> violations)
    {
        var steps = schedule.Drain();
        for (var i = 0; i < steps.Count; i++)
            _ = Replay(steps[i], nodes[^1], down, string.Empty, nodes.Length);

        if (down.Stopped.Count != 0 || down.Isolated.Count != 0 || schedule.Healthy().Length != nodes.Length)
            violations.Add($"seed {seed}: the drain left {down.Stopped.Count} stopped and {down.Isolated.Count} isolated nodes");
    }

    private static string? Replay(FaultStep step, string anchor, (HashSet<string> Stopped, HashSet<string> Isolated) down, string leader, int minority)
    {
        string? problem;
        switch (step.Kind)
        {
            case FaultKind.Stop:
            case FaultKind.AbruptStop:
            case FaultKind.Isolate:
                problem = down.Stopped.Contains(step.NodeId) || down.Isolated.Contains(step.NodeId) ? "the fault strikes a node that is not running and connected" : null;
                _ = (step.Kind == FaultKind.Isolate ? down.Isolated : down.Stopped).Add(step.NodeId);
                problem ??= string.Equals(step.NodeId, anchor, StringComparison.Ordinal) ? "the fault strikes the anchor" : null;
                problem ??= step.HitsLeader != string.Equals(step.NodeId, leader, StringComparison.Ordinal) ? "the leader flag is wrong" : null;
                break;
            case FaultKind.Restart:
                problem = down.Stopped.Remove(step.NodeId) ? null : "a restart strikes a node that is not stopped";
                break;
            case FaultKind.Heal:
                problem = down.Isolated.Count == 0 ? "a heal without a cut link" : null;
                down.Isolated.Clear();
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(step), step.Kind, "Unsupported enum value.");
        }

        return problem ?? (down.Stopped.Count + down.Isolated.Count > minority ? "more than a minority is down" : null);
    }

    private static bool AgreesOnNodesUp(FaultSchedule schedule, string[] nodes, (HashSet<string> Stopped, HashSet<string> Isolated) down)
    {
        var running = new List<string>();
        var healthy = new List<string>();
        foreach (var node in nodes)
        {
            if (down.Stopped.Contains(node))
                continue;

            running.Add(node);
            if (!down.Isolated.Contains(node))
                healthy.Add(node);
        }

        return Same(running, [.. schedule.Running()]) && Same(healthy, [.. schedule.Healthy()]);
    }

    private static bool Same<T>(List<T> left, List<T> right)
        where T : IEquatable<T>
    {
        if (left.Count != right.Count)
            return false;

        for (var i = 0; i < left.Count; i++)
        {
            if (!left[i].Equals(right[i]))
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
