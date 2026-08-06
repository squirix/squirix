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
/// </remarks>
internal sealed class GroupRecovery : IAsyncDisposable
{
    private readonly GroupComposition _composition;
    private readonly Dictionary<string, IFollowerLog> _logs = new(StringComparer.Ordinal);
    private readonly string _persistenceRoot;

    internal GroupRecovery(string persistenceRoot, GroupComposition composition)
    {
        _persistenceRoot = persistenceRoot ?? throw new ArgumentNullException(nameof(persistenceRoot));
        _composition = composition ?? throw new ArgumentNullException(nameof(composition));
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        foreach (var log in _logs.Values)
            await log.DisposeAsync().ConfigureAwait(false);

        _logs.Clear();
    }

    /// <summary>Opens a follower log for <paramref name="groupId" /> without materializing storage yet.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="faultHooks">Optional fault-injection seam.</param>
    /// <returns>A follower log for the group.</returns>
    internal FollowerLog CreateLog(string groupId, IFollowerLogFaultHooks? faultHooks = null) => new(_persistenceRoot, groupId, _composition, faultHooks);

    /// <summary>Opens and recovers the committed prefix for every group in the local composition.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every local group log is open and recovered.</returns>
    internal async Task RecoverAllAsync(CancellationToken cancellationToken)
    {
        await DisposeAsync().ConfigureAwait(false);
        _logs.Clear();

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

    /// <summary>Returns the recovered follower log for <paramref name="groupId" />, or <see langword="null" />.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The recovered follower log, or <see langword="null" /> when the group is not open.</returns>
    internal IFollowerLog? GetLog(string groupId) => _logs.GetValueOrDefault(groupId);

    /// <summary>Returns the recovered committed records for <paramref name="groupId" />.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The committed records for the group.</returns>
    internal IReadOnlyList<FollowerLogEntry> GetCommittedRecords(string groupId) => GetLog(groupId) is { } log ? log.GetCommittedEntries() : [];
}
