using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>Independent controls for the complete single-writer register checker.</summary>
public sealed class RegisterLinearizabilityTests
{
    /// <summary>A timed-out write cannot be undone by an acknowledged write already observed before it.</summary>
    [Test]
    public async Task RejectsResurrectionMissedByPairwiseCheck()
    {
        RegisterWrite[] writes = [new("k", 1, 10, 20, false), new("k", 2, 30, 40, true)];
        RegisterRead[] reads = [new("k", 50, 60, 2), new("k", 70, 80, 1), new("k", 90, 100, 2)];
        _ = await Assert.That(RegisterHistory.Check(writes, reads)).IsEmpty();
        _ = await Assert.That(RegisterLinearizability.Check(writes, reads, CancellationToken.None)).HasSingleItem();
    }

    /// <summary>Ambiguous writes can be omitted, applied late, or observed before a later acknowledged write.</summary>
    /// <param name="first">The first observation.</param>
    /// <param name="second">The second observation.</param>
    /// <param name="valid">Whether the history has a legal sequential explanation.</param>
    [Test]
    [Arguments(2L, 2L, true)]
    [Arguments(2L, 1L, true)]
    [Arguments(1L, 2L, false)]
    [Arguments(0L, 2L, false)]
    public async Task ModelsAmbiguousCompletion(long first, long second, bool valid)
    {
        RegisterWrite[] writes = [new("k", 1, 10, 20, false), new("k", 2, 30, 40, true)];
        RegisterRead[] reads = [new("k", 50, 60, first), new("k", 70, 80, second)];
        _ = await Assert.That(RegisterLinearizability.Check(writes, reads, CancellationToken.None).Count == 0).IsEqualTo(valid);
    }

    /// <summary>Equal endpoints overlap, whereas a one-tick gap enforces real-time order.</summary>
    /// <param name="start">The read invocation timestamp.</param>
    /// <param name="valid">Whether observing the initial value is legal.</param>
    [Test]
    [Arguments(20L, true)]
    [Arguments(21L, false)]
    public async Task RespectsRealTimeBoundary(long start, bool valid) =>
        _ = await Assert.That(RegisterLinearizability.Check([new("k", 1, 10, 20, true)], [new("k", start, 30, 0)], CancellationToken.None).Count == 0).IsEqualTo(valid);

    /// <summary>Concurrent observations need not follow invocation order.</summary>
    [Test]
    public async Task SearchesOrdersForOverlappingReads()
    {
        RegisterWrite[] writes = [new("k", 1, 10, 50, true), new("k", 2, 60, 90, true)];
        RegisterRead[] reads = [new("k", 65, 100, 2), new("k", 70, 80, 1)];
        _ = await Assert.That(RegisterLinearizability.Check(writes, reads, CancellationToken.None)).IsEmpty();
    }

    /// <summary>Keys have independent initial states, even when another key wrote the observed value.</summary>
    [Test]
    public async Task PartitionsByKeyAndAcceptsEmptyHistory()
    {
        _ = await Assert.That(RegisterLinearizability.Check([], [], CancellationToken.None)).IsEmpty();
        _ = await Assert.That(RegisterLinearizability.Check([new("a", 1, 10, 20, true)], [new("b", 30, 40, 0)], CancellationToken.None)).IsEmpty();
        _ = await Assert.That(RegisterLinearizability.Check([new("a", 1, 10, 20, true)], [new("b", 30, 40, 1)], CancellationToken.None)).HasSingleItem();
    }

    /// <summary>A canceled check never reports a passing or failing consistency verdict.</summary>
    [Test]
    public async Task CancellationDoesNotProduceVerdict()
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        OperationCanceledException? failure = null;
        try
        {
            _ = RegisterLinearizability.Check([], [], cancellation.Token);
        }
        catch (OperationCanceledException exception)
        {
            failure = exception;
        }

        _ = await Assert.That(failure).IsNotNull();
        _ = await Assert.That(failure!.CancellationToken).IsEqualTo(cancellation.Token);
    }
}
