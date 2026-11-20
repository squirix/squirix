namespace Squirix.Server.Logging;

/// <summary>Ordered severity levels used by node configuration.</summary>
internal enum LogLevel
{
    /// <summary>Detailed protocol tracing.</summary>
    Trace = 0,

    /// <summary>Diagnostic detail.</summary>
    Debug = 1,

    /// <summary>Routine operational events.</summary>
    Information = 2,

    /// <summary>Degraded service or unexpected conditions.</summary>
    Warning = 3,

    /// <summary>Operation failures.</summary>
    Error = 4,

    /// <summary>Failures that prevent continued service.</summary>
    Critical = 5,

    /// <summary>No events.</summary>
    None = 6,
}
