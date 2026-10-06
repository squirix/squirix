using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
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
    private readonly ILoggerFactory _loggerFactory;
    private readonly FollowerLogOptions? _options;
    private readonly string _root;
    private readonly int _replicaCount;
    private readonly ReadOnlyMemory<byte> _fingerprint;
    private int _disposed;
    private FrozenDictionary<string, GroupState>? _groups;
    private int _opened;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupRegistry" /> class.</summary>
    /// <param name="root">Exclusive node data directory owning the group storage.</param>
    /// <param name="groupIds">Replica group identifiers served by this node.</param>
    /// <param name="replicaCount">Fixed replica count shared by every group.</param>
    /// <param name="fingerprint">Static topology fingerprint shared by the replica set.</param>
    /// <param name="generation">Static configuration generation shared by the replica set.</param>
    /// <param name="loggerFactory">Factory creating the logger handed to every group log.</param>
    /// <param name="options">Follower log options applied to every group log.</param>
    /// <exception cref="ArgumentException">Thrown when a group identifier is missing or duplicated.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the replica count is out of range.</exception>
    internal ReplicaGroupRegistry(
        string root,
        IReadOnlyList<string> groupIds,
        int replicaCount,
        ReadOnlyMemory<byte> fingerprint,
        ulong generation,
        ILoggerFactory loggerFactory,
        FollowerLogOptions? options = null)
    {
        ArgumentException.ThrowIfNullOrEmpty(root);
        ArgumentNullException.ThrowIfNull(groupIds);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        if (replicaCount < 1 || replicaCount > PolicyOptions.MaxReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count is out of range.");
        ValidateGroupIds(groupIds);
        if (fingerprint.IsEmpty)
            throw new ArgumentException("Topology fingerprint must not be empty.", nameof(fingerprint));

        _root = root;
        _groupIds = [.. groupIds];
        _replicaCount = replicaCount;
        _fingerprint = fingerprint;
        _generation = generation;
        _loggerFactory = loggerFactory;
        _options = options;
    }

    /// <summary>Gets the replica group identifiers served by this node.</summary>
    /// <returns>The group identifiers fixed at construction.</returns>
    internal IReadOnlyList<string> GroupIds => _groupIds;

    /// <inheritdoc />
    /// <remarks>
    /// The logs are disposed concurrently, so they share one shutdown budget instead of stacking theirs, and a log stuck in a
    /// flush neither delays nor hides the disposal of the others; each log reports its own leak. Every log is disposed even when
    /// another fails, and such a failure surfaces only after all of them finished.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        var groups = _groups;
        if (groups == null)
            return;

        var logs = groups.Values;
        var disposals = new Task[logs.Length];
        for (var i = 0; i < logs.Length; i++)
            disposals[i] = logs[i].Log.DisposeAsync().AsTask();

        await Task.WhenAll(disposals).ConfigureAwait(false);
    }

    /// <summary>Gets the participation gate for a served group.</summary>
    /// <param name="id">Replica group identifier.</param>
    /// <returns>The participation gate.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the registry is not opened.</exception>
    /// <exception cref="KeyNotFoundException">Thrown when this node does not serve the group.</exception>
    internal ReplicaEligibility EligibilityFor(string id) => _groups == null ? throw new InvalidOperationException("Replica group registry is not opened.") : _groups[id].Eligibility;

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
                groups.Add(_groupIds[i], await OpenGroupAsync(_groupIds[i], cancellationToken).ConfigureAwait(false));
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

    /// <summary>Determines whether a served group retains an outcome, resolved or still in flight, for a client operation.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cacheName">The cache name, the scope of client operations.</param>
    /// <param name="operationId">The operation identifier.</param>
    /// <returns>
    /// <see langword="true" /> when a commit of the operation would replay or wait for a recorded outcome instead of deciding, and also
    /// while the outcomes of the log are not rebuilt since it was opened: the commit, which rebuilds them first, then decides.
    /// </returns>
    internal bool HasRecordedOutcome(string groupId, string cacheName, string operationId) =>
        TryGetLog(groupId, out var log) && (!log.Idempotency.OutcomesRebuilt || log.Idempotency.Lookup(cacheName, operationId, [], out _) != GroupIdempotencyLookup.Miss);

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

    /// <summary>Creates and opens the log of one group, adopts the configured topology, and builds its participation gate.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The opened group state; the log is disposed when opening fails.</returns>
    private async Task<GroupState> OpenGroupAsync(string groupId, CancellationToken cancellationToken)
    {
        FollowerLog? log = null;
        try
        {
            log = new FollowerLog(_root, groupId, GroupComposition.Create(groupId), _loggerFactory.CreateLogger<FollowerLog>(), _options);
            await log.OpenAsync(cancellationToken).ConfigureAwait(false);

            // Refused before the committer applies anything from the log.
            await log.AdoptTopologyAsync(_fingerprint, _generation, cancellationToken).ConfigureAwait(false);
            var eligibility = new ReplicaEligibility(_replicaCount);

            // A group with no durable progress starts with every member ready: there is nothing
            // to disagree about, and verified appends keep the quorum honest afterwards. Any
            // durable state means a restart, which stays recovering until a repair session
            // verifies it.
            var status = await log.GetStatusAsync(cancellationToken).ConfigureAwait(false);
            if (status is { LastLogIndex: 0, CommitIndex: 0 } && log.SnapshotPath == null)
            {
                var zero = new ReplicaProgress(1, 0, 0, 0, 0, _fingerprint, _generation, 0);
                for (var r = 0; r < eligibility.ReplicaCount; r++)
                    _ = eligibility.TryMarkReady(r, in zero, in zero);
            }

            var state = new GroupState(log, eligibility);
            log = null;
            return state;
        }
        finally
        {
            if (log != null)
                await log.DisposeAsync().ConfigureAwait(false);
        }
    }

    [Immutable]
    private readonly record struct GroupState(FollowerLog Log, ReplicaEligibility Eligibility);
}
