using Microsoft.Extensions.Logging;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Logging for test nodes keeps Squirix categories verbose.</summary>
internal static class TestHostLogging
{
    private const LogLevel SquirixLevel = LogLevel.Debug;
    private const string SquirixCategory = "Squirix";

    /// <summary>Adds the console provider and sets category levels; the console is the only provider so each host runs a single logger thread.</summary>
    /// <param name="builder">The host logging builder, already cleared of default providers.</param>
    internal static void Configure(ILoggingBuilder builder)
    {
        _ = builder.SetMinimumLevel(SquirixLevel);
        _ = builder.AddFilter(SquirixCategory, SquirixLevel);
        _ = builder.AddConsole();
    }
}
