using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Result of capturing the group snapshot at the durable applied index of a log.</summary>
/// <param name="Outcome">Whether the snapshot was captured, or why it was not.</param>
/// <param name="Snapshot">The captured snapshot; meaningful only when <paramref name="Outcome" /> is <see cref="GroupSnapshotCaptureOutcome.Captured" />.</param>
[Immutable]
internal readonly record struct GroupSnapshotCapture(GroupSnapshotCaptureOutcome Outcome, GroupSnapshot Snapshot)
{
    /// <summary>Creates the result of a capture that was refused.</summary>
    /// <param name="outcome">The reason; not <see cref="GroupSnapshotCaptureOutcome.Captured" />.</param>
    /// <returns>The refusal result, holding no snapshot.</returns>
    internal static GroupSnapshotCapture Refused(GroupSnapshotCaptureOutcome outcome) => new(outcome, default);
}
