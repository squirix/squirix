namespace Squirix.Server.Logging;

/// <summary>Maps a subsystem name to its routing key for the centralized log sink.</summary>
internal static class LogRouting
{
    /// <summary>Returns the routing key for <paramref name="subsystem" />.</summary>
    /// <param name="subsystem">The subsystem name.</param>
    /// <returns>The routing key, equal to <paramref name="subsystem" />.</returns>
    internal static string Route(string subsystem) => subsystem;
}
