namespace Squirix.Server.Storage.Journaling;

internal enum JournalWorkKind
{
    /// <summary>Append a framed journal record.</summary>
    Append = 0,

    /// <summary>Run a durability checkpoint (fsync + complete the item's own ack) without an append payload.</summary>
    DurabilityCheckpoint = 2,

    /// <summary>Shut down the journal I/O thread.</summary>
    Shutdown = 3,
}
