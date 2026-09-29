using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A committed or recovered log record whose effect contradicts its outcome is never applied: the committer refuses to start on it,
/// memory and the applied index stay as they were, and the refusal is counted.
/// </summary>
public sealed class ReplicaInconsistentRecordTests : ServerUnitTestBase
{
    private const string MetricName = "squirix_replication_inconsistent_records_total";

    /// <summary>A committed entry that is inconsistent stops the catch-up at start: memory is untouched and the applied index does not advance.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InconsistentCommittedEntryStopsCatchUp(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-inconsistent-committed");
        await SeedRecordAsync(dir, await InconsistentRecordAsync(cancellationToken), true, cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var total = new long[1];
        using var listener = CountMetric(meter, total);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache, TimeProvider.System, new ReplicationMetrics(meter));

        var error = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(committer.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken));
        _ = await Assert.That(error.Message).Contains("inconsistent", StringComparison.Ordinal);
        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((1UL, 0UL));
        _ = await Assert.That(Interlocked.Read(ref total[0])).IsEqualTo(1L);
    }

    /// <summary>An uncommitted tail entry that is inconsistent stops the committer from starting, on every attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InconsistentRecoveredTailRefusesStart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-inconsistent-tail");
        await SeedRecordAsync(dir, await InconsistentRecordAsync(cancellationToken), false, cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var total = new long[1];
        using var listener = CountMetric(meter, total);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), cache, TimeProvider.System, new ReplicationMetrics(meter));

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken));
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(committer.CommitSetAsync(NewOperationId(), "cache", "k3", Entry("k3"), cancellationToken));

        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
        var status = await StatusAsync(registry, cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((1UL, 0UL, 0UL));
        _ = await Assert.That(Interlocked.Read(ref total[0])).IsEqualTo(1L);
    }

    /// <summary>The same refused record, attempted again and again, is counted once; a different record is counted again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedRefusalsCountOnce(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test");
        var total = new long[1];
        using var listener = CountMetric(meter, total);
        var applier = new ReplicaLeaderApplier(new StubCache(), "n1", "n1", NullLogger.Instance, new ReplicationMetrics(meter));
        var inconsistent = await InconsistentRecordAsync(cancellationToken);
        var bad = ReplicaLogCodec.Encode(in inconsistent);

        for (var attempt = 0; attempt < 5; attempt++)
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(applier.ApplyAsync(1UL, bad, cancellationToken));

        _ = await Assert.That(Interlocked.Read(ref total[0])).IsEqualTo(1L);
        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(0UL);
    }

    /// <summary>A failure of the cache write itself is not reported as an inconsistent record, even when it is an invalid-data failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteFailureIsNotInconsistentRecord(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test");
        var total = new long[1];
        using var listener = CountMetric(meter, total);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback(static (_, _, _, _, _) => ValueTask.FromException(new InvalidDataException("entry exceeds the payload limit")));
        var applier = new ReplicaLeaderApplier(cache.Instance(), "n1", "n1", NullLogger.Instance, new ReplicationMetrics(meter));
        var valid = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System).PrepareSet(NewOperationId(), "cache", "k1", Entry("k1"), 1UL);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(applier.ApplyAsync(1UL, valid.CanonicalPayload, cancellationToken));

        _ = await Assert.That(Interlocked.Read(ref total[0])).IsEqualTo(0L);
    }

    /// <summary>An uncommitted tail entry whose entry payload does not decode stops the committer from starting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UndecodableTailPayloadRefusesStart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-owner-undecodable-tail");
        var record = await InconsistentRecordAsync(cancellationToken) with
        {
            MutationPayload = new byte[] { 1 },
            OutcomePayload = ReplicaOutcomeCodec.Encode(true, ReadOnlyMemory<byte>.Empty),
        };
        await SeedRecordAsync(dir, record, false, cancellationToken);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), new StubCache());

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken));
    }

    private static MeterListener CountMetric(Meter meter, long[] total)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, target) =>
            {
                if (ReferenceEquals(instrument.Meter, meter) && string.Equals(instrument.Name, MetricName, StringComparison.Ordinal))
                    target.EnableMeasurementEvents(instrument, total);
            },
        };
        listener.SetMeasurementEventCallback<long>(
            static (_, value, _, state) =>
            {
                if (state is long[] counts)
                    _ = Interlocked.Add(ref counts[0], value);
            });
        listener.Start();
        return listener;
    }

    private static async Task<ReplicaLogRecord> InconsistentRecordAsync(CancellationToken cancellationToken)
    {
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System);
        var prepared = await factory.PrepareTryAddAsync(NewOperationId(), "cache", "k1", Entry("k1"), 1UL, cancellationToken);
        var record = ReplicaLogCodec.Decode(prepared.CanonicalPayload);
        _ = await Assert.That(record).IsNotNull();

        // The record writes an entry but its outcome says the add was rejected.
        return record!.Value with { OutcomePayload = ReplicaOutcomeCodec.Encode(false, ReadOnlyMemory<byte>.Empty) };
    }
}
