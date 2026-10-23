using System;
using System.Globalization;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Shared shapes of the group idempotency byte bound tests: every identity has the same length, so every regular outcome the same size.</summary>
internal static class OutcomeBytesKit
{
    internal const string Scope = "client";

    /// <summary>The payload of a regular outcome, as the removed value of a remove.</summary>
    internal const int PayloadBytes = 100;

    /// <summary>The number of regular outcomes the byte bound of <see cref="Bounded" /> holds.</summary>
    internal const int OutcomesInBound = 3;

    /// <summary>A capacity no test reaches, so only the byte bound evicts.</summary>
    internal const int RoomyCapacity = 1024;

    internal static readonly TimeSpan Retention = TimeSpan.FromMinutes(15);

    internal static readonly TimeSpan Floor = TimeSpan.FromMinutes(1);

    internal static readonly DateTimeOffset Epoch = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);

    /// <summary>The snapshot bytes of one regular outcome.</summary>
    internal static readonly long OutcomeSize = GroupSnapshotStore.OutcomeEncodedLength(Scope, Id(0), 1, PayloadBytes);

    /// <summary>The payload of an outcome larger than the whole byte bound.</summary>
    internal static readonly int OversizedPayload = Convert.ToInt32(OutcomeSize * OutcomesInBound * 2);

    internal static GroupIdempotencyState Bounded(TimeProvider clock) => new(RoomyCapacity, Retention, clock, Floor, OutcomeSize * OutcomesInBound);

    internal static string Id(int number) => "op-" + number.ToString("D2", CultureInfo.InvariantCulture);

    internal static GroupIdempotencyRecord Outcome(FakeTimeProvider clock, string id, ulong index, int payloadBytes)
    {
        var resolvedUtc = clock.GetUtcNow().UtcDateTime;
        return new GroupIdempotencyRecord(Scope, id, new byte[] { 1 }, new byte[payloadBytes], GroupRecordKind.UserMutation, resolvedUtc, resolvedUtc, index, 1UL);
    }

    internal static GroupIdempotencyReserveResult Pin(GroupIdempotencyState state, string id, ulong index, int payloadBytes) =>
        state.Reserve(Scope, id, [1], GroupRecordKind.UserMutation, index, 1UL, new GroupReserveOptions(false, payloadBytes));

    internal static void Resolve(GroupIdempotencyState state, string id, ulong index, int payloadBytes)
    {
        _ = Pin(state, id, index, payloadBytes);
        _ = state.TryResolve(Scope, id, new byte[payloadBytes], index, 1UL);
    }

    internal static void Commit(GroupIdempotencyState state, FakeTimeProvider clock, string id, ulong index, TimeSpan decided)
    {
        var resolvedUtc = clock.GetUtcNow().UtcDateTime - decided;
        var record = new GroupIdempotencyRecord(Scope, id, new byte[] { 1 }, new byte[PayloadBytes], GroupRecordKind.UserMutation, resolvedUtc, resolvedUtc, index, 1UL);
        state.RecordCommittedOutcome(in record);
    }

    internal static GroupIdempotencyLookup Lookup(GroupIdempotencyState state, string id) => state.Lookup(Scope, id, [1], out _);
}
