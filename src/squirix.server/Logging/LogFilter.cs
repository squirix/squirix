using Microsoft.Extensions.Logging;

namespace Squirix.Server.Logging;

/// <summary>Decides whether a log entry at a given level should be emitted by the server.</summary>
internal static class LogFilter
{
    /// <summary>Returns <see langword="true" /> when <paramref name="level" /> is at or above information severity.</summary>
    /// <param name="level">The log level to evaluate.</param>
    /// <returns><see langword="true" /> when <paramref name="level" /> is information or higher.</returns>
    internal static bool ShouldLog(LogLevel level) => level >= LogLevel.Information;
}
