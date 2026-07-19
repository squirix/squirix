namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>Identifies a journal writer operation for distributed tracing.</summary>
internal enum JournalOperationKind
{
    /// <summary>A remove journal record.</summary>
    Remove = 0,

    /// <summary>A remove-expiration journal record.</summary>
    RemoveExpiration = 1,

    /// <summary>A touch-expiration journal record.</summary>
    TouchExpiration = 2,

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
}
