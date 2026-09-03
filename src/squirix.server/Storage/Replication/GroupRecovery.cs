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
    private readonly string _persistenceRoot;
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
    internal async ValueTask<IReadOnlyList<FollowerLogEntry>> GetCommittedRecordsAsync(string groupId, CancellationToken cancellationToken) =>
        GetLog(groupId) is { } log ? await log.GetCommittedEntriesAsync(cancellationToken).ConfigureAwait(false) : [];

    /// <summary>Returns the recovered follower log for <paramref name="groupId" />, or <see langword="null" />.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The recovered follower log, or <see langword="null" /> when the group is not open.</returns>
    internal IFollowerLog? GetLog(string groupId) => Volatile.Read(ref _logs).GetValueOrDefault(groupId);

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
    /// <returns>A task that completes when all open logs are disposed.</returns>
    private async Task CloseLogsAsync()
    {
        var previous = Volatile.Read(ref _logs);
        Volatile.Write(ref _logs, new Dictionary<string, IFollowerLog>(StringComparer.Ordinal));
        foreach (var log in previous.Values)
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
