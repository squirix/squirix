using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owns the durable follower logs and participation gates for every replica group of one node.</summary>
/// <remarks>
/// One log exists per group; the same log serves local owner appends and follower replication appends.
/// Logs are created and opened explicitly through <see cref="OpenAsync" /> (usually from node startup).
/// Before that every lookup fails closed: unknown groups are refused and the gates are unavailable.
/// </remarks>
internal sealed class ReplicaGroupRegistry : IAsyncDisposable
{
    private readonly ulong _generation;
    private readonly string[] _groupIds;
    private readonly FollowerLogOptions? _options;
    private readonly string _persistenceRoot;
    private readonly int _replicaCount;
    private readonly ReadOnlyMemory<byte> _topologyFingerprint;
    private int _disposed;
    private FrozenDictionary<string, GroupState>? _groups;
    private int _opened;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupRegistry" /> class.</summary>
    /// <param name="persistenceRoot">Exclusive node data directory owning the group storage.</param>
    /// <param name="groupIds">Replica group identifiers served by this node.</param>
    /// <param name="replicaCount">Fixed replica count shared by every group.</param>
    /// <param name="topologyFingerprint">Static topology fingerprint shared by the replica set.</param>
    /// <param name="generation">Static configuration generation shared by the replica set.</param>
    /// <param name="options">Follower log options applied to every group log.</param>
    /// <exception cref="ArgumentException">Thrown when a group identifier is missing or duplicated.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the replica count is out of range.</exception>
    internal ReplicaGroupRegistry(
        string persistenceRoot,
        IReadOnlyList<string> groupIds,
        int replicaCount,
        ReadOnlyMemory<byte> topologyFingerprint,
        ulong generation,
        FollowerLogOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(persistenceRoot);
        ArgumentNullException.ThrowIfNull(groupIds);
        if (replicaCount < 1 || replicaCount > PolicyOptions.MaxReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count is out of range.");
        ValidateGroupIds(groupIds);
        if (topologyFingerprint.IsEmpty)
            throw new ArgumentException("Topology fingerprint must not be empty.", nameof(topologyFingerprint));

        _persistenceRoot = persistenceRoot;
        _groupIds = [.. groupIds];
        _replicaCount = replicaCount;
        _topologyFingerprint = topologyFingerprint;
        _generation = generation;
        _options = options;
    }

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        var groups = _groups;
        if (groups == null)
            return;

        foreach (var state in groups.Values)
            await state.Log.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Gets the participation gate for a served group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The participation gate.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the registry is not opened.</exception>
    /// <exception cref="KeyNotFoundException">Thrown when this node does not serve the group.</exception>
    internal ReplicaEligibility EligibilityFor(string groupId)
    {
        if (_groups == null)
            throw new InvalidOperationException("Replica group registry is not opened.");
        return _groups[groupId].Eligibility;
    }

    /// <summary>Creates and opens every group log for durable replication.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when every log is ready.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the registry is already opened.</exception>
    internal async Task OpenAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Exchange(ref _opened, 1) != 0)
            throw new InvalidOperationException("Replica group registry is already opened.");

        var groups = new Dictionary<string, GroupState>(_groupIds.Length, StringComparer.Ordinal);
        try
        {
            for (var i = 0; i < _groupIds.Length; i++)
            {
                FollowerLog? log = null;
                try
                {
                    log = new FollowerLog(_persistenceRoot, _groupIds[i], GroupComposition.Create(_groupIds[i]), _options);
                    await log.OpenAsync(cancellationToken).ConfigureAwait(false);
                    var eligibility = new ReplicaEligibility(_replicaCount);

                    // A group with no durable progress starts with every member ready: there is nothing
                    // to disagree about, and verified appends keep the quorum honest afterwards. Any
                    // durable state means a restart, which stays recovering until a repair session
                    // verifies it.
                    var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
                    if (status is { LastLogIndex: 0, CommitIndex: 0 } && log.SnapshotPath == null)
                    {
                        var zero = new ReplicaProgress(1, 0, 0, 0, 0, _topologyFingerprint, _generation, 0);
                        for (var r = 0; r < eligibility.ReplicaCount; r++)
                            _ = eligibility.TryMarkReady(r, in zero, in zero);
                    }

                    groups.Add(_groupIds[i], new GroupState(log, eligibility));
                    log = null;
                }
                finally
                {
                    if (log != null)
                        await log.DisposeAsync().ConfigureAwait(false);
                }
            }
        }
        catch
        {
            foreach (var state in groups.Values)
                await state.Log.DisposeAsync().ConfigureAwait(false);
            throw;
        }

        _groups = groups.ToFrozenDictionary(StringComparer.Ordinal);
    }

    /// <summary>Gets the follower log for a served group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="log">The follower log when this node serves the group; otherwise <see langword="null" />.</param>
    /// <returns><see langword="true" /> when this node serves the group; otherwise <see langword="false" />.</returns>
    internal bool TryGetLog(string groupId, [NotNullWhen(true)] out IFollowerLog? log)
    {
        if (_groups?.TryGetValue(groupId, out var state) == true)
        {
            log = state.Log;
            return true;
        }

        log = null;
        return false;
    }

    private static void ValidateGroupIds(IReadOnlyList<string> groupIds)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < groupIds.Count; i++)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(groupIds[i]);
            if (!seen.Add(groupIds[i]))
                throw new ArgumentException("Replica group identifiers must be distinct.", nameof(groupIds));
        }
    }

    [Immutable]
    private readonly record struct GroupState(FollowerLog Log, ReplicaEligibility Eligibility);
}
