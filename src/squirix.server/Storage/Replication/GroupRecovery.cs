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
    private readonly string _persistenceRoot;
    private readonly Lock _gate = new();

    /// <summary>
    /// The open follower logs are published as an immutable snapshot so readers observe a fully-built map and never a
    /// partially-populated one while <see cref="RecoverAllAsync" /> / <see cref="CloseLogsAsync" /> swap the collection.
    /// </summary>
    private IReadOnlyDictionary<string, IFollowerLog> _logs = new Dictionary<string, IFollowerLog>(StringComparer.Ordinal);
    private bool _disposed;

    internal GroupRecovery(string persistenceRoot, GroupComposition composition)
    {
        _persistenceRoot = persistenceRoot ?? throw new ArgumentNullException(nameof(persistenceRoot));
        _composition = composition ?? throw new ArgumentNullException(nameof(composition));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Volatile.Read(ref _disposed))
            return;

        Volatile.Write(ref _disposed, true);
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
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed), this);
        await CloseLogsAsync().ConfigureAwait(false);

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

        var publish = true;
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed))
            {
                publish = false;
            }
            else
            {
                var snapshot = new Dictionary<string, IFollowerLog>(opened.Count, StringComparer.Ordinal);
                for (var i = 0; i < opened.Count; i++)
                    snapshot[opened[i].GroupId] = opened[i];
                Volatile.Write(ref _logs, snapshot);
            }
        }

        if (publish)
            return;

        // The coordinator was disposed while recovery opened the logs; dispose them so they do not leak.
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
}
