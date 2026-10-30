namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Reason a memory-growing operation is refused.</summary>
internal enum AdmissionRejection
{
    /// <summary>The operation is admitted.</summary>
    None = 0,

    /// <summary>Known growth is refused while current usage is critical.</summary>
    CriticalPressure = 1,

    /// <summary>Known growth would bring usage into the critical band.</summary>
    EstimatedLimit = 2,

    /// <summary>Unbounded growth is refused while current usage is critical.</summary>
    UnknownSize = 3,
}
