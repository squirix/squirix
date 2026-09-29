namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Identifies a journal writer operation for distributed tracing.</summary>
/// <remarks>Values 1 and 2 belonged to the retired delta frames and are not reused: every cache-entry frame is a put of the whole entry or a remove.</remarks>
internal enum JournalOperationKind
{
    /// <summary>A remove journal record.</summary>
    Remove = 0,

    /// <summary>A put journal record.</summary>
    Put = 3,

    /// <summary>Await durability commit completion.</summary>
    AwaitDurabilityCommit = 4,

    /// <summary>Wait for journal startup to complete.</summary>
    WaitForStartup = 5,

    /// <summary>Exclusive maintenance work under the journal gate.</summary>
    MaintenanceExclusive = 6,

    /// <summary>Snapshot cut coordination.</summary>
    SnapshotCut = 7,

    /// <summary>Work executed under the snapshot barrier.</summary>
    UnderSnapshotBarrier = 8,

    /// <summary>Idempotency outcome record (durable replay state for mutating RPCs).</summary>
    IdempotencyOutcome = 9,

    /// <summary>Write-ahead idempotency intent whose mutation may have committed but whose outcome is unknown.</summary>
    IdempotencyStarted = 10,
}
