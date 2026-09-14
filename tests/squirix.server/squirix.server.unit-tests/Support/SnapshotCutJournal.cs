using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Threading;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Shared <see cref="IJournalCoordinator" /> stub with fixed snapshot-cut coordinates.</summary>
internal sealed class SnapshotCutJournal : IJournalCoordinator
{
    /// <summary>Initializes a new instance of the <see cref="SnapshotCutJournal" /> class.</summary>
    /// <param name="segmentIndex">Fixed current segment index.</param>
    /// <param name="nextSequence">Fixed next sequence.</param>
    internal SnapshotCutJournal(int segmentIndex, ulong nextSequence)
    {
        CurrentSegmentIndex = segmentIndex;
        NextSequence = nextSequence;
    }

    // Subscriptions are accepted and dropped: the snapshot paths under test never subscribe,
    // so there is no backing field to raise from.

    /// <inheritdoc />
    public event EventHandler? OnAppended
    {
        add => _ = value;
        remove => _ = value;
    }

    /// <inheritdoc />
    public long AppendedBytes => 1024;

    /// <inheritdoc />
    public long AppendedOps => 1;

    /// <inheritdoc />
    public int CurrentSegmentIndex { get; }

    /// <inheritdoc />
    public bool HasFlushLoopFailure => false;

    /// <inheritdoc />
    public long HighWaterBytes => 0;

    /// <inheritdoc />
    public QuiescenceGate InFlightApplyGate => new();

    /// <inheritdoc />
    public bool IsJournalGroupCommitEnabled => false;

    /// <inheritdoc />
    public long MaxBytes => 0;

    /// <inheritdoc />
    public ulong NextSequence { get; }

    /// <inheritdoc />
    public double RecentAppendLatencyMs => 0;

    /// <inheritdoc />
    public long UsedBytes => 0;

    /// <inheritdoc />
    public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask DisposeAsync() => default;

    /// <inheritdoc />
    public ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public async ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
        CancellationToken cancellationToken)
    {
        var barrier = await captureUnderBarrier(state, 1, cancellationToken).ConfigureAwait(false);
        return await buildOutsideBarrier(state, 1, barrier, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        Func<TState, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken) => default;

    /// <inheritdoc />
    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => default;
}
