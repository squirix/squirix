using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Replication;

/// <summary>Startup recovery of the published snapshot: checks it against the durable metadata and restores its baseline.</summary>
internal static class FollowerLogSnapshotRecovery
{
    /// <summary>Restores the idempotency baseline from a compatible published snapshot and returns its included index.</summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="cancellationToken">A token to observe while reading.</param>
    /// <returns>The snapshot's included index, or <c language="csharp">0</c> when no snapshot is published.</returns>
    /// <exception cref="InvalidDataException">
    /// The published snapshot is corrupt, belongs to another group, or conflicts
    /// with the durable topology metadata.
    /// </exception>
    internal static async Task<ulong> RestoreSnapshotBaseAsync(FollowerLogJournal journal, IFollowerLogContext owner, CancellationToken cancellationToken)
    {
        if (!journal.Snapshot.SnapshotExists)
            return 0UL;

        try
        {
            var snapshot = await journal.Snapshot.ReadPublishedAsync(cancellationToken).ConfigureAwait(false);
            if (snapshot == null)
                return 0UL;

            if (!string.Equals(snapshot.Value.GroupId, owner.GroupId, StringComparison.Ordinal))
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot belongs to group '{snapshot.Value.GroupId}'.");
            }

            if (FollowerLogSnapshot.SnapshotTopologyMismatch(owner, snapshot.Value) != null)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot topology or configuration generation conflicts with durable metadata.");
            }

            // A snapshot whose commit index falls below its included index is malformed: the committed prefix it
            // claims to cover is internally inconsistent, and adopting its boundary would let LastAppliedIndex
            // exceed CommitIndex. ReconcileSnapshotMetadataAsync assumes the decoder already rejected this; guard
            // here so recovery fails readiness before persisting an incoherent watermark, matching
            // ValidateInstallEligibility on the installation path.
            if (snapshot.Value.CommitIndex < snapshot.Value.LastIncludedIndex)
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException(
                    $"Replica group '{owner.GroupId}' snapshot commit index '{snapshot.Value.CommitIndex}' is below its included index '{snapshot.Value.LastIncludedIndex}'.");
            }

            // Terms start at 1. A zero-included term collides with the unverifiable-term sentinel used by
            // TermAtApplied and would make DivergentBoundary discard the entire durable suffix.
            if (snapshot.Value is { LastIncludedIndex: > 0UL, LastIncludedTerm: 0UL })
            {
                owner.Readiness = FollowerLogReadiness.Failed;
                throw new InvalidDataException($"Replica group '{owner.GroupId}' snapshot included term is zero at index '{snapshot.Value.LastIncludedIndex}'.");
            }

            // Reconcile and durably persist the snapshot metadata BEFORE mutating in-memory recovery state. If the
            // persisting is canceled (OperationCanceledException, preserved as retryable by PersistMetaOrFailReadinessAsync)
            // or fails, the instance is left without partially restored idempotency/baseline, so a retry of OpenAsync on
            // the same instance cannot operate on inconsistent state. See F37.
            await ReconcileSnapshotMetadataAsync(journal, owner, snapshot.Value, cancellationToken).ConfigureAwait(false);
            owner.Idempotency.RestoreFromSnapshot(snapshot.Value.CommittedOutcomes, snapshot.Value.CapturedUtc, []);

            // Recovery rebuilds both indexes from the durable log after this point; the baseline is
            // restored without pruning because the walk owns the index lifecycle.
            owner.RestoreBaseline(new SnapshotBaseline(snapshot.Value.LastIncludedIndex, snapshot.Value.LastIncludedTerm));
            return snapshot.Value.LastIncludedIndex;
        }
        catch (InvalidDataException)
        {
            owner.Readiness = FollowerLogReadiness.Failed;
            throw;
        }
    }

    /// <summary>
    /// Reconciles the published snapshot's term, vote, topology fingerprint, configuration generation, and
    /// watermarks with the durable metadata, persisting the repaired metadata before readiness so a replica
    /// restarted after a crash between snapshot publication and metadata advance does not load stale state.
    /// </summary>
    /// <param name="journal">The paired in-memory journal state.</param>
    /// <param name="owner">The log being recovered.</param>
    /// <param name="snapshot">The published snapshot loaded from the disk.</param>
    /// <param name="cancellationToken">A token to observe while persisting metadata.</param>
    private static async Task ReconcileSnapshotMetadataAsync(FollowerLogJournal journal, IFollowerLogContext owner, GroupSnapshot snapshot, CancellationToken cancellationToken)
    {
        var included = snapshot.LastIncludedIndex;
        var commit = Math.Max(owner.Meta.CommitIndex, Math.Min(snapshot.CommitIndex, included));
        var candidate = owner.Meta;
        var changed = false;

        if (commit > candidate.CommitIndex)
        {
            candidate = candidate with { CommitIndex = commit };
            changed = true;
        }

        // A snapshot is authoritative for the covered prefix: a higher included term advances the durable
        // term and clears any vote cast in the older term before readiness, mirroring InstallAsync.
        var termRaised = snapshot.LastIncludedTerm > candidate.CurrentTerm;
        if (termRaised)
        {
            candidate = candidate with { CurrentTerm = snapshot.LastIncludedTerm, VotedFor = string.Empty };
            changed = true;
        }

        if (candidate.TopologyFingerprint.IsEmpty && !snapshot.TopologyFingerprint.IsEmpty)
        {
            candidate = candidate with { TopologyFingerprint = snapshot.TopologyFingerprint };
            changed = true;
        }

        if (snapshot.ConfigurationGeneration > candidate.ConfigurationGeneration)
        {
            candidate = candidate with { ConfigurationGeneration = snapshot.ConfigurationGeneration };
            changed = true;
        }

        if (!changed)
            return;

        // A raised term is mirrored into both slots, like every term step; other repairs go to one slot.
        var persist = termRaised
            ? FollowerLogAppend.PersistMirroredMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken)
            : FollowerLogAppend.PersistMetaOrFailReadinessAsync(journal, owner, candidate, cancellationToken);
        await persist.ConfigureAwait(false);
        owner.Meta = candidate;
    }
}
