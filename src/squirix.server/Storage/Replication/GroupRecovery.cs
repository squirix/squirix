using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Coordinates opening and recovery of the replica-group follower logs owned by the local node.</summary>
/// <remarks>
/// Storage-only orchestration: it opens a log per local group and exposes the committed records through the
/// storage contract. Applying committed records to memory is the responsibility of an outer layer and is
/// intentionally not performed here.
/// <para>
/// In the current milestone the coordinator is registered but <see cref="RecoverAllAsync" /> is not invoked
/// from any production path: with the static local composition being empty, a production call would open no
/// groups. Recovery wiring is introduced together with group-membership derivation (see M8-05).
/// </para>
/// </remarks>
internal sealed class GroupRecovery : IAsyncDisposable
{
    private readonly GroupComposition _composition;
    private readonly Lock _gate = new();

    /// <summary>Outstanding leases per published log, guarded by <see cref="_gate" />.</summary>
    private readonly Dictionary<IFollowerLog, int> _leaseCounts = [];

    private readonly string _persistenceRoot;

    /// <summary>Logs displaced from the map while leased, guarded by <see cref="_gate" />.</summary>
    private readonly HashSet<IFollowerLog> _retired = [];

    private int _disposed;

    /// <summary>
    /// The open follower logs are published as an immutable snapshot so readers observe a fully-built map and never a
    /// partially-populated one while <see cref="RecoverAllAsync" /> / <see cref="CloseLogsAsync" /> swap the collection.
    /// </summary>
    private IReadOnlyDictionary<string, IFollowerLog> _logs = new Dictionary<string, IFollowerLog>(StringComparer.Ordinal);

    internal GroupRecovery(string persistenceRoot, GroupComposition composition)
    {
        ArgumentNullException.ThrowIfNull(persistenceRoot);
        ArgumentNullException.ThrowIfNull(composition);
        _persistenceRoot = persistenceRoot;
        _composition = composition;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        await CloseLogsAsync().ConfigureAwait(false);
    }

    /// <summary>Returns the recovered committed records for <paramref name="groupId" />.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The committed records for the group.</returns>
    internal async ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedRecordsAsync(string groupId, CancellationToken cancellationToken)
    {
        var lease = AcquireLog(groupId);
        if (lease == null)
            return [];

        try
        {
            return await lease.Log.GetCommittedEntriesAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Returns the recovered follower log for <paramref name="groupId" />, or <see langword="null" />.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The recovered follower log, or <see langword="null" /> when the group is not open.</returns>
    internal IFollowerLog? GetLog(string groupId) => Volatile.Read(ref _logs).GetValueOrDefault(groupId);

    /// <summary>Acquires a lease on the recovered follower log for <paramref name="groupId" />.</summary>
    /// <remarks>
    /// A leased log is not disposed by <see cref="CloseLogsAsync" /> until the last lease is released, so the
    /// fetch-then-use sequence cannot race disposal. Prefer this over <see cref="GetLog" />, which returns a
    /// point-in-time snapshot without lifetime protection.
    /// </remarks>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>A lease on the log, or <see langword="null" /> when the group is not open or the coordinator is disposed.</returns>
    internal LogLease? AcquireLog(string groupId)
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return null;

            if (!Volatile.Read(ref _logs).TryGetValue(groupId, out var log))
                return null;

            _leaseCounts[log] = _leaseCounts.TryGetValue(log, out var count) ? count + 1 : 1;
            return new LogLease(log, this);
        }
    }

    /// <summary>Releases a lease acquired by <see cref="AcquireLog" />, disposing retired logs whose last lease ends.</summary>
    /// <param name="log">The leased follower log.</param>
    /// <returns>A task that completes when the release (and any resulting disposal) finishes.</returns>
    internal async ValueTask ReleaseAsync(IFollowerLog log)
    {
        IFollowerLog? toDispose = null;
        lock (_gate)
        {
            if (!_leaseCounts.TryGetValue(log, out var count) || count <= 0)
                return;

            if (count == 1)
                _ = _leaseCounts.Remove(log);
            else
                _leaseCounts[log] = count - 1;

            if (count == 1 && _retired.Remove(log))
                toDispose = log;
        }

        if (toDispose != null)
            await toDispose.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Opens and recovers the committed prefix for every group in the local composition.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every local group log is open and recovered.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the coordinator is already disposed.</exception>
    internal async Task RecoverAllAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

        // A re-recovery replaces the currently open logs, so the previous set is closed first.
        await CloseLogsAsync().ConfigureAwait(false);

        // Open the committed prefix of every local group; on failure the already-opened logs are disposed.
        var opened = await OpenLogsAsync(cancellationToken).ConfigureAwait(false);

        // Publish the recovered logs atomically so concurrent readers never observe a partial set.
        if (await TryPublishAsync(opened).ConfigureAwait(false))
            return;

        // The coordinator was disposed while the logs were being opened; dispose them so they do not leak.
        foreach (var log in opened)
            await log.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Disposes and forgets every currently open follower log.</summary>
    /// <remarks>
    /// Logs with outstanding leases are retired from the map but disposed only when the last lease is
    /// released, so leased callers never observe disposal out from under a fetch-then-use sequence.
    /// </remarks>
    /// <returns>A task that completes when all unleased open logs are disposed.</returns>
    private async Task CloseLogsAsync()
    {
        List<IFollowerLog> toDispose;
        lock (_gate)
        {
            var previous = Volatile.Read(ref _logs);
            Volatile.Write(ref _logs, new Dictionary<string, IFollowerLog>(StringComparer.Ordinal));
            toDispose = new List<IFollowerLog>(previous.Count);
            foreach (var pair in previous)
            {
                if (_leaseCounts.TryGetValue(pair.Value, out var count) && count > 0)
                    _ = _retired.Add(pair.Value);
                else
                    toDispose.Add(pair.Value);
            }
        }

        foreach (var log in toDispose)
            await log.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Opens a follower log for <paramref name="groupId" /> without materializing storage yet.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>A follower log for the group.</returns>
    private FollowerLog CreateLog(string groupId) => new(_persistenceRoot, groupId, _composition);

    /// <summary>Opens and recovers every group log in the composition, disposing the already-opened set on failure.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The recovered follower logs.</returns>
    private async Task<List<IFollowerLog>> OpenLogsAsync(CancellationToken cancellationToken)
    {
        var opened = new List<IFollowerLog>();
        try
        {
            foreach (var groupId in _composition.GroupIds)
            {
                var log = CreateLog(groupId);
                opened.Add(log);
                await log.OpenAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            foreach (var log in opened)
                await log.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        return opened;
    }

    /// <summary>Atomically publishes <paramref name="opened" /> as the current snapshot, unless the coordinator was disposed.</summary>
    /// <param name="opened">The recovered follower logs to publish.</param>
    /// <returns><see langword="true" /> when the snapshot was published; <see langword="false" /> when the coordinator is disposed.</returns>
    private ValueTask<bool> TryPublishAsync(List<IFollowerLog> opened)
    {
        lock (_gate)
        {
            // The coordinator was disposed while the logs were being opened, so nothing may be published.
            if (Volatile.Read(ref _disposed) != 0)
                return ValueTask.FromResult(false);

            var snapshot = new Dictionary<string, IFollowerLog>(opened.Count, StringComparer.Ordinal);
            for (var i = 0; i < opened.Count; i++)
                snapshot[opened[i].GroupId] = opened[i];
            Volatile.Write(ref _logs, snapshot);
            return ValueTask.FromResult(true);
        }
    }
}
