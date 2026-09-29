using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The effect of a replicated record must agree with its outcome; any other combination is refused before memory is touched.</summary>
public sealed class ReplicaEffectResolutionTests : ServerUnitTestBase
{
    private const string Invalid = "Invalid";

    private static readonly byte[] EntryBytes = ReplicaCacheApplier.EncodeEntry(new NodeCacheEntry<object?>("v"));
    private static readonly byte[] KeyBytes = [107];

    /// <summary>An outcome that does not decode is refused.</summary>
    [Test]
    public async Task UndecodableOutcomeIsRefused()
    {
        var record = Record(ReplicaMutationKinds.Set, true, true, 0, false) with { OutcomePayload = new byte[] { 1, 2 } };

        var error = NodeExceptionAssert.For<InvalidDataException>().Throws(record, static candidate => _ = ReplicaCacheApplier.ResolveEffect(in candidate));

        _ = await Assert.That(error.Message).Contains("undecodable", StringComparison.Ordinal);
    }

    /// <summary>An upsert whose entry payload does not decode is refused.</summary>
    [Test]
    public async Task UndecodablePayloadIsRefused()
    {
        var record = Record(ReplicaMutationKinds.Set, true, true, 0, false) with { MutationPayload = new byte[] { 1 } };

        var error = NodeExceptionAssert.For<InvalidDataException>().Throws(record, static candidate => _ = ReplicaCacheApplier.ResolveEffect(in candidate));

        _ = await Assert.That(error.Message).Contains("undecodable", StringComparison.Ordinal);
    }

    /// <summary>A deadline beyond the largest representable time is refused as inconsistent instead of overflowing.</summary>
    [Test]
    public async Task OutOfRangeDeadlineIsRefused()
    {
        var record = Record(ReplicaMutationKinds.Set, true, true, DateTime.MaxValue.Ticks, false) with { ExpiresUtcTicks = long.MaxValue };

        var error = NodeExceptionAssert.For<InvalidDataException>().Throws(record, static candidate => _ = ReplicaCacheApplier.ResolveEffect(in candidate));

        _ = await Assert.That(error.Message).Contains("out of range", StringComparison.Ordinal);
    }

    /// <summary>A refusal names the log index, the kind and the applied flag of the record.</summary>
    [Test]
    public async Task RefusalNamesEntryKindAndAppliedFlag()
    {
        var record = Record(ReplicaMutationKinds.Set, false, false, 0, false) with { LogIndex = 42UL };

        var error = NodeExceptionAssert.For<InvalidDataException>().Throws(record, static candidate => _ = ReplicaCacheApplier.ResolveEffect(in candidate));

        _ = await Assert.That(error.Message).Contains("42", StringComparison.Ordinal);
        _ = await Assert.That(error.Message).Contains(ReplicaMutationKinds.Set, StringComparison.Ordinal);
        _ = await Assert.That(error.Message).Contains("False", StringComparison.Ordinal);
    }

    /// <summary>Every kind, applied flag, payload, deadline and previous entry resolves to the effect of the table or is refused.</summary>
    /// <param name="kind">The mutation kind.</param>
    /// <param name="applied">The applied flag of the outcome.</param>
    /// <param name="hasPayload">Whether the record carries an entry.</param>
    /// <param name="ticks">The pinned deadline ticks of the record.</param>
    /// <param name="hasPrevious">Whether the outcome carries a previous entry.</param>
    /// <param name="expected">The expected effect name, or Invalid.</param>
    [Test]
    [Arguments(ReplicaMutationKinds.Set, true, true, 0L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.Set, true, true, 100L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.Set, false, false, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Set, true, false, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Set, true, true, 0L, true, Invalid)]
    [Arguments(ReplicaMutationKinds.Set, true, true, -1L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.TryAdd, true, true, 100L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.TryAdd, true, false, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.TryAdd, false, false, 0L, false, "Unchanged")]
    [Arguments(ReplicaMutationKinds.TryAdd, false, true, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.TryAdd, false, false, 100L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Update, true, true, 0L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.Update, true, true, 100L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.Update, true, false, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Update, false, false, 0L, false, "Unchanged")]
    [Arguments(ReplicaMutationKinds.Update, false, true, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Update, false, false, 100L, true, Invalid)]
    [Arguments(ReplicaMutationKinds.Touch, true, true, 100L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.Touch, true, true, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Touch, true, false, 100L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Touch, false, false, 0L, false, "Unchanged")]
    [Arguments(ReplicaMutationKinds.Touch, false, false, 100L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.RemoveExpiration, true, true, 0L, false, "Upsert")]
    [Arguments(ReplicaMutationKinds.RemoveExpiration, true, true, 100L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.RemoveExpiration, true, false, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.RemoveExpiration, false, false, 0L, false, "Unchanged")]
    [Arguments(ReplicaMutationKinds.RemoveExpiration, false, true, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Remove, true, false, 0L, true, "Delete")]
    [Arguments(ReplicaMutationKinds.Remove, false, false, 0L, false, "Delete")]
    [Arguments(ReplicaMutationKinds.Remove, true, false, 0L, false, Invalid)]
    [Arguments(ReplicaMutationKinds.Remove, false, false, 0L, true, Invalid)]
    [Arguments(ReplicaMutationKinds.Remove, true, true, 0L, true, Invalid)]
    [Arguments(ReplicaMutationKinds.Remove, true, false, 100L, true, Invalid)]
    [Arguments("unknown", true, true, 0L, false, Invalid)]
    public async Task ResolveEffectFollowsTable(string kind, bool applied, bool hasPayload, long ticks, bool hasPrevious, string expected)
    {
        var record = Record(kind, applied, hasPayload, ticks, hasPrevious);

        if (string.Equals(expected, Invalid, StringComparison.Ordinal))
        {
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(record, static candidate => _ = ReplicaCacheApplier.ResolveEffect(in candidate));
            return;
        }

        _ = await Assert.That(ReplicaCacheApplier.ResolveEffect(in record)).IsEqualTo(Enum.Parse<ReplicaEffectKind>(expected));
    }

    /// <summary>An inconsistent record is never applied: memory is left untouched.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InconsistentRecordNeverReachesMemory(CancellationToken cancellationToken)
    {
        var cache = new ReplicaOwnerTestKit.StubCache();
        var record = Record(ReplicaMutationKinds.Set, false, true, 0, false);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (cache, record, cancellationToken),
            static state => _ = ReplicaCacheApplier.ApplyAsync(state.cache, state.record, state.cancellationToken));

        _ = await Assert.That(cache.Applied.IsEmpty).IsTrue();
    }

    private static ReplicaLogRecord Record(string kind, bool applied, bool hasPayload, long ticks, bool hasPrevious) => new(
        1UL,
        1UL,
        "op",
        "scope",
        new byte[] { 1 },
        "UserMutation",
        "cache",
        KeyBytes,
        kind,
        hasPayload ? EntryBytes : ReadOnlyMemory<byte>.Empty,
        ReplicaOutcomeCodec.Encode(applied, hasPrevious ? new byte[] { 1 } : ReadOnlyMemory<byte>.Empty),
        ticks,
        0,
        0,
        0);
}
