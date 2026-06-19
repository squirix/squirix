namespace Squirix.Server.Storage.Journaling.PipelinedWal.Backends.Pipelined;

internal enum WalWorkKind
{
    /// <summary>Append a framed journal record.</summary>
    Append,

    /// <summary>Flush durability to disk.</summary>
    Flush,

    /// <summary>Shut down the WAL thread.</summary>
    Shutdown,

    /// <summary>Begin exclusive maintenance (flush and release segment).</summary>
    MaintenanceBegin,

    /// <summary>End exclusive maintenance (re-sync manifest state).</summary>
    MaintenanceEnd,
}
