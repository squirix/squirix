using System;
using System.IO;

namespace Squirix.Server.Storage.Journaling;

/// <summary>The faults on the journal thread that fail the journal pipeline instead of crashing the process.</summary>
internal static class JournalPipelineFault
{
    /// <summary>Tells whether a fault that escaped the journal thread loop fails the journal pipeline.</summary>
    /// <param name="exception">The fault.</param>
    /// <returns><see langword="true" /> for storage, access, disposal, state and timeout faults.</returns>
    internal static bool Is(Exception exception) =>
        exception is IOException or ObjectDisposedException or InvalidOperationException or TimeoutException or UnauthorizedAccessException;
}
