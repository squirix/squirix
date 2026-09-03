using System;
using System.Collections.Generic;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Tracks identity-verified contiguous durable progress for a fixed replica group.</summary>
[ThreadSafe]
internal sealed class ReplicaCommitQuorum
{
    private readonly Dictionary<int, HashSet<ulong>> _futureAcks = [];
    private readonly ulong[] _matchIndexes;
    private readonly Lock _sync = new();

    /// <summary>Initializes a new instance of the <see cref="ReplicaCommitQuorum" /> class.</summary>
    /// <param name="replicaCount">Fixed the replica count, including the leader.</param>
    /// <param name="initialMatchIndex">Initial contiguous durable index for every replica.</param>
    internal ReplicaCommitQuorum(int replicaCount, ulong initialMatchIndex = 0)
    {
        if (replicaCount is < 1 or > PolicyOptions.MaxReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), $"Replica count must be between 1 and {PolicyOptions.MaxReplicaCount}.");

        ReplicaCount = replicaCount;
        RequiredCopies = (replicaCount / 2) + 1;
        _matchIndexes = new ulong[replicaCount];
        if (initialMatchIndex == 0)
            return;

        for (var i = 0; i < _matchIndexes.Length; i++)
            _matchIndexes[i] = initialMatchIndex;
    }

    internal int ReplicaCount { get; }

    internal int RequiredCopies { get; }

    /// <summary>Returns the highest majority-backed contiguous index, never below the current commit index.</summary>
    /// <param name="currentCommitIndex">Current durable group commit index.</param>
    /// <param name="lastLogIndex">Highest local durable log index eligible for commit.</param>
    /// <returns>The highest contiguous index backed by a majority.</returns>
    internal ulong FindCommitIndex(ulong currentCommitIndex, ulong lastLogIndex)
    {
        if (lastLogIndex <= currentCommitIndex)
            return currentCommitIndex;

        lock (_sync)
        {
            for (var candidate = lastLogIndex; candidate > currentCommitIndex; candidate--)
            {
                var copies = 0;
                for (var i = 0; i < _matchIndexes.Length; i++)
                {
                    if (_matchIndexes[i] >= candidate)
                        copies++;
                }

                if (copies >= RequiredCopies)
                    return candidate;
            }

            return currentCommitIndex;
        }
    }

    /// <summary>Returns the highest contiguous durable index recorded for one replica.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns>The replica match index.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="replicaIndex" /> is outside the fixed group.</exception>
    internal ulong MatchIndexFor(int replicaIndex)
    {
        // Bounds check against ReplicaCount (always equals _matchIndexes.Length):
        // the array size is fixed at construction, so no lock is needed here.
        if (replicaIndex < 0 || replicaIndex >= ReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaIndex));

        lock (_sync)
            return _matchIndexes[replicaIndex];
    }

    /// <summary>Records a verified acknowledgement for one replica slot.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <param name="acknowledgement">Durable acknowledgement to verify.</param>
    /// <param name="expected">Prepared mutation whose identity must match.</param>
    /// <returns>
    /// <see langword="true" /> when the acknowledgement identity is valid and recorded: contiguously, or buffered
    /// when it arrives ahead of its missing prefix (the buffered index counts toward the match only after the
    /// prefix lands and catch-up runs); otherwise, <see langword="false" />.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="expected" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="replicaIndex" /> is outside the fixed group.</exception>
    internal bool TryRecord(int replicaIndex, in ReplicaDurableAcknowledgement acknowledgement, PreparedReplicaMutation expected)
    {
        ArgumentNullException.ThrowIfNull(expected);
        if (replicaIndex < 0 || replicaIndex >= ReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaIndex));

        if (!acknowledgement.IsDurable || !acknowledgement.IsReady || !string.Equals(acknowledgement.GroupId, expected.GroupId, StringComparison.Ordinal) ||
            acknowledgement.Term != expected.Term || acknowledgement.LogIndex != expected.LogIndex || acknowledgement.PayloadChecksum != expected.PayloadChecksum ||
            !acknowledgement.OperationFingerprint.Span.SequenceEqual(expected.OperationFingerprint.Span))
            return false;

        lock (_sync)
        {
            var current = _matchIndexes[replicaIndex];
            if (acknowledgement.LogIndex <= current)
                return acknowledgement.LogIndex == current;

            if (acknowledgement.LogIndex == current + 1)
            {
                _matchIndexes[replicaIndex] = acknowledgement.LogIndex;
                AdvanceThroughBuffered(replicaIndex);
                return true;
            }

            if (!_futureAcks.TryGetValue(replicaIndex, out var buffered))
            {
                buffered = [];
                _futureAcks[replicaIndex] = buffered;
            }

            _ = buffered.Add(acknowledgement.LogIndex);
            return true;
        }
    }

    /// <summary>Advances one replica through buffered future indexes while they form a contiguous run.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <remarks>Must be called under <see cref="_sync" /> after a contiguous advance.</remarks>
    private void AdvanceThroughBuffered(int replicaIndex)
    {
        if (!_futureAcks.TryGetValue(replicaIndex, out var buffered))
            return;

        while (_matchIndexes[replicaIndex] != ulong.MaxValue && buffered.Remove(_matchIndexes[replicaIndex] + 1))
            _matchIndexes[replicaIndex]++;

        if (buffered.Count == 0)
            _ = _futureAcks.Remove(replicaIndex);
    }
}
