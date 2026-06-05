namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Identifies a journal writer operation for distributed tracing.
/// </summary>
internal enum JournalOperationKind
{
    /// <summary>A remove journal record.</summary>
    Remove,

    /// <summary>A remove-expiration journal record.</summary>
    RemoveExpiration,

    /// <summary>A touch-expiration journal record.</summary>
    TouchExpiration,

    /// <summary>A put journal record.</summary>
    Put,

    /// <summary>Await durability commit completion.</summary>
    AwaitDurabilityCommit,

    /// <summary>Wait for journal startup to complete.</summary>
    WaitForStartup,

    /// <summary>Exclusive maintenance work under the journal gate.</summary>
    MaintenanceExclusive,

    /// <summary>Snapshot cut coordination.</summary>
    SnapshotCut,

    /// <summary>Work executed under the snapshot barrier.</summary>
    UnderSnapshotBarrier,
}
