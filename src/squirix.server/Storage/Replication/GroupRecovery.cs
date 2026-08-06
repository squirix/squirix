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
    private readonly Dictionary<string, IFollowerLog> _logs = new(StringComparer.Ordinal);
    private readonly string _persistenceRoot;

    private bool _disposed;

    internal GroupRecovery(string persistenceRoot, GroupComposition composition)
    {
        _persistenceRoot = persistenceRoot ?? throw new ArgumentNullException(nameof(persistenceRoot));
        _composition = composition ?? throw new ArgumentNullException(nameof(composition));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;
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
    internal IFollowerLog? GetLog(string groupId) => _logs.GetValueOrDefault(groupId);

    /// <summary>Opens and recovers the committed prefix for every group in the local composition.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every local group log is open and recovered.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when the coordinator is already disposed.</exception>
    internal async Task RecoverAllAsync(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
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

        for (var i = 0; i < opened.Count; i++)
            _logs[opened[i].GroupId] = opened[i];
    }

    /// <summary>Disposes and forgets every currently open follower log.</summary>
    /// <returns>A task that completes when all open logs are disposed.</returns>
    private async Task CloseLogsAsync()
    {
        foreach (var log in _logs.Values)
            await log.DisposeAsync().ConfigureAwait(false);

        _logs.Clear();
    }

    /// <summary>Opens a follower log for <paramref name="groupId" /> without materializing storage yet.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="faultHooks">Optional fault-injection seam.</param>
    /// <returns>A follower log for the group.</returns>
    private FollowerLog CreateLog(string groupId, IFollowerLogFaultHooks? faultHooks = null) => new(_persistenceRoot, groupId, _composition, faultHooks);
}
