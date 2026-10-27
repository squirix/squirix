namespace Squirix.Server.Threading;

/// <summary>How a <see cref="TimedWait" /> ended.</summary>
internal enum TimedWaitResult
{
    /// <summary>The awaited condition holds.</summary>
    Signaled = 0,

    /// <summary>The delay elapsed before the condition held.</summary>
    TimedOut = 1,

    /// <summary>The caller's token was canceled before the condition held.</summary>
    Canceled = 2,
}
