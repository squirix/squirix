using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owns fail-closed participation state for the fixed slots of one replica group.</summary>
[ThreadSafe]
internal sealed class ReplicaEligibility
{
    private readonly ReplicaProgress[] _progress;
    private readonly ReplicaParticipantState[] _states;
    private readonly Lock _sync = new();

    /// <summary>Initializes a new instance of the <see cref="ReplicaEligibility" /> class with every participant recovering.</summary>
    /// <param name="replicaCount">Fixed replica count.</param>
    internal ReplicaEligibility(int replicaCount)
    {
        if (replicaCount is < 1 or > PolicyOptions.MaxReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), $"Replica count must be between 1 and {PolicyOptions.MaxReplicaCount}.");

        ReplicaCount = replicaCount;
        _progress = new ReplicaProgress[replicaCount];
        _states = new ReplicaParticipantState[replicaCount];
    }

    /// <summary>Gets the fixed replica count.</summary>
    internal int ReplicaCount { get; }

    /// <summary>Returns whether the participant may be promoted to leader.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns><see langword="true" /> only for a verified ready participant.</returns>
    internal bool CanBePromoted(int replicaIndex) => IsReady(replicaIndex);

    /// <summary>Returns whether the participant may count toward a write quorum.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns><see langword="true" /> only for a verified ready participant.</returns>
    internal bool CanCountInWriteQuorum(int replicaIndex) => IsReady(replicaIndex);

    /// <summary>Returns whether the participant may vote in an election.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns><see langword="true" /> only for a verified ready participant.</returns>
    internal bool CanVote(int replicaIndex) => IsReady(replicaIndex);

    /// <summary>Moves a participant back to recovering and clears unverified progress.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    internal void MarkRecovering(int replicaIndex)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
        {
            _progress[replicaIndex] = default;
            _states[replicaIndex] = ReplicaParticipantState.Recovering;
        }
    }

    /// <summary>Quarantines a participant until an explicit recovery restarts it.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    internal void Quarantine(int replicaIndex)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
            _states[replicaIndex] = ReplicaParticipantState.Quarantined;
    }

    /// <summary>Returns the last retained progress report for a participant.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns>The retained progress report.</returns>
    internal ReplicaProgress ProgressFor(int replicaIndex)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
            return _progress[replicaIndex];
    }

    /// <summary>Returns the participant state.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <returns>The participant state.</returns>
    internal ReplicaParticipantState StateFor(int replicaIndex)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
            return _states[replicaIndex];
    }

    /// <summary>Records monotonic catch-up progress and excludes the participant from authority paths.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <param name="progress">Verified progress report.</param>
    /// <returns><see langword="true" /> when the report was accepted.</returns>
    internal bool TryMarkCatchingUp(int replicaIndex, in ReplicaProgress progress)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
        {
            if (_states[replicaIndex] == ReplicaParticipantState.Quarantined)
                return false;

            if (!progress.IsValid)
            {
                // A participant that reports structurally invalid progress cannot be trusted to hold
                // its ready verdict: demote it back to catch-up instead of letting it keep authority.
                if (_states[replicaIndex] == ReplicaParticipantState.Ready)
                {
                    _progress[replicaIndex] = default;
                    _states[replicaIndex] = ReplicaParticipantState.CatchingUp;
                }

                return false;
            }

            var previous = _progress[replicaIndex];
            if (!previous.TopologyFingerprint.IsEmpty && !progress.MatchesTopology(in previous))
            {
                _states[replicaIndex] = ReplicaParticipantState.Quarantined;
                return false;
            }

            _states[replicaIndex] = ReplicaParticipantState.CatchingUp;
            if (!previous.TopologyFingerprint.IsEmpty &&
                (progress.NextIndex < previous.NextIndex || progress.MatchIndex < previous.MatchIndex || progress.CommitIndex < previous.CommitIndex ||
                 progress.AppliedIndex < previous.AppliedIndex || progress.LastTerm < previous.LastTerm))
                return false;

            _progress[replicaIndex] = progress.WithOwnedFingerprint();
            return true;
        }
    }

    /// <summary>Transitions a participant to ready only after an exact leader-side verification.</summary>
    /// <param name="replicaIndex">Zero-based replica slot.</param>
    /// <param name="observed">Participant's observed durable state.</param>
    /// <param name="expected">Leader-verified durable state.</param>
    /// <returns><see langword="true" /> when the participant became ready.</returns>
    internal bool TryMarkReady(int replicaIndex, in ReplicaProgress observed, in ReplicaProgress expected)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
        {
            if (_states[replicaIndex] == ReplicaParticipantState.Quarantined)
                return false;

            if (!observed.IsValid || !expected.IsValid)
            {
                if (_states[replicaIndex] == ReplicaParticipantState.Ready)
                {
                    _progress[replicaIndex] = default;
                    _states[replicaIndex] = ReplicaParticipantState.CatchingUp;
                }

                return false;
            }

            var previous = _progress[replicaIndex];
            if ((!previous.TopologyFingerprint.IsEmpty && !observed.MatchesTopology(in previous)) || !observed.MatchesTopology(in expected))
            {
                _states[replicaIndex] = ReplicaParticipantState.Quarantined;
                return false;
            }

            if ((!previous.TopologyFingerprint.IsEmpty &&
                 (observed.NextIndex < previous.NextIndex || observed.MatchIndex < previous.MatchIndex || observed.CommitIndex < previous.CommitIndex ||
                  observed.AppliedIndex < previous.AppliedIndex || observed.LastTerm < previous.LastTerm)) || !observed.Matches(in expected))
            {
                _states[replicaIndex] = ReplicaParticipantState.CatchingUp;
                return false;
            }

            _progress[replicaIndex] = observed.WithOwnedFingerprint();
            _states[replicaIndex] = ReplicaParticipantState.Ready;
            return true;
        }
    }

    private bool IsReady(int replicaIndex)
    {
        ValidateIndex(replicaIndex);
        lock (_sync)
            return _states[replicaIndex] == ReplicaParticipantState.Ready;
    }

    private void ValidateIndex(int replicaIndex)
    {
        if (replicaIndex < 0 || replicaIndex >= ReplicaCount)
            throw new ArgumentOutOfRangeException(nameof(replicaIndex));
    }
}
