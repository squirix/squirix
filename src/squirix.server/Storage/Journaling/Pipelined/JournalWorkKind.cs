namespace Squirix.Server.Storage.Journaling.Pipelined;

internal enum JournalWorkKind
{
    /// <summary>Append a framed journal record.</summary>
    Append,

    /// <summary>Flush durability to disk.</summary>
    Flush,

    /// <summary>Shut down the journal I/O thread.</summary>
    Shutdown,

    /// <summary>Begin exclusive maintenance (flush and release segment).</summary>
    MaintenanceBegin,

    /// <summary>End exclusive maintenance (re-sync manifest state).</summary>
    MaintenanceEnd,
}
