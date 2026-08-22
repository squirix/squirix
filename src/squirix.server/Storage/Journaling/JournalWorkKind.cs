namespace Squirix.Server.Storage.Journaling;

internal enum JournalWorkKind
{
    /// <summary>Append a framed journal record.</summary>
    Append = 0,

    /// <summary>Append a framed journal record and complete one durability ack after fsync.</summary>
    AppendWithDurability = 1,

    /// <summary>Run a durability checkpoint (fsync + complete the item's own ack) without an append payload.</summary>
    DurabilityCheckpoint = 2,

    /// <summary>Shut down the journal I/O thread.</summary>
    Shutdown = 3,

    /// <summary>Begin exclusive maintenance (flush and release segment).</summary>
    MaintenanceBegin = 4,

    /// <summary>End exclusive maintenance (re-sync manifest state).</summary>
    MaintenanceEnd = 5,
}
