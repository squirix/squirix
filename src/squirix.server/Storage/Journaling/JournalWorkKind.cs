namespace Squirix.Server.Storage.Journaling;

internal enum JournalWorkKind
{
    /// <summary>Append a framed journal record.</summary>
    Append = 0,

    /// <summary>Append a framed journal record and complete one durability waiter after fsync.</summary>
    AppendWithDurability = 1,

    /// <summary>Flush durability to disk.</summary>
    Flush = 2,

    /// <summary>Run a durability checkpoint (fsync + complete waiters) without an append payload.</summary>
    DurabilityCheckpoint = 3,

    /// <summary>Shut down the journal I/O thread.</summary>
    Shutdown = 4,

    /// <summary>Begin exclusive maintenance (flush and release segment).</summary>
    MaintenanceBegin = 5,

    /// <summary>End exclusive maintenance (re-sync manifest state).</summary>
    MaintenanceEnd = 6,
}
