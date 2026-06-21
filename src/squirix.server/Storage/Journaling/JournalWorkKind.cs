namespace Squirix.Server.Storage.Journaling;

internal enum JournalWorkKind
{
    /// <summary>Append a framed journal record.</summary>
    Append,

    /// <summary>Append a framed journal record and complete one durability waiter after fsync.</summary>
    AppendWithDurability,

    /// <summary>Flush durability to disk.</summary>
    Flush,

    /// <summary>Run a durability checkpoint (fsync + complete waiters) without an append payload.</summary>
    DurabilityCheckpoint,

    /// <summary>Shut down the journal I/O thread.</summary>
    Shutdown,

    /// <summary>Begin exclusive maintenance (flush and release segment).</summary>
    MaintenanceBegin,

    /// <summary>End exclusive maintenance (re-sync manifest state).</summary>
    MaintenanceEnd,
}
