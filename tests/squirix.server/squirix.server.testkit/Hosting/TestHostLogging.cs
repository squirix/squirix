using Microsoft.Extensions.Logging;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Logging setup shared by test node hosts: Squirix categories stay verbose, framework per-request categories are limited to warnings,
/// and the general Kestrel category stays at information so a failed loopback bind is visible.
/// </summary>
internal static class TestHostLogging
{
    private const string AspNetCoreCategory = "Microsoft.AspNetCore";
    private const string ExtensionsHostingCategory = "Microsoft.Extensions.Hosting";
    private const string GrpcCategory = "Grpc";
    private const string HostingCategory = "Microsoft.Hosting";
    private const LogLevel FrameworkLevel = LogLevel.Warning;
    private const string KestrelBadRequestsCategory = "Microsoft.AspNetCore.Server.Kestrel.BadRequests";
    private const string KestrelCategory = "Microsoft.AspNetCore.Server.Kestrel";
    private const string KestrelConnectionsCategory = "Microsoft.AspNetCore.Server.Kestrel.Connections";
    private const string KestrelHttpsCategory = "Microsoft.AspNetCore.Server.Kestrel.Https";
    private const string KestrelHttp2Category = "Microsoft.AspNetCore.Server.Kestrel.Http2";
    private const string KestrelHttp3Category = "Microsoft.AspNetCore.Server.Kestrel.Http3";
    private const LogLevel KestrelLevel = LogLevel.Information;
    private const LogLevel SquirixLevel = LogLevel.Debug;
    private const string SquirixCategory = "Squirix";

    /// <summary>Adds the console provider and sets category levels; the console is the only provider so each host runs a single logger thread.</summary>
    /// <param name="builder">The host logging builder, already cleared of default providers.</param>
    internal static void Configure(ILoggingBuilder builder)
    {
        _ = builder.SetMinimumLevel(SquirixLevel);
        _ = builder.AddFilter(AspNetCoreCategory, FrameworkLevel);
        _ = builder.AddFilter(KestrelCategory, KestrelLevel);
        _ = builder.AddFilter(KestrelBadRequestsCategory, FrameworkLevel);
        _ = builder.AddFilter(KestrelConnectionsCategory, FrameworkLevel);
        _ = builder.AddFilter(KestrelHttpsCategory, FrameworkLevel);
        _ = builder.AddFilter(KestrelHttp2Category, FrameworkLevel);
        _ = builder.AddFilter(KestrelHttp3Category, FrameworkLevel);
        _ = builder.AddFilter(ExtensionsHostingCategory, FrameworkLevel);
        _ = builder.AddFilter(GrpcCategory, FrameworkLevel);
        _ = builder.AddFilter(HostingCategory, FrameworkLevel);
        _ = builder.AddFilter(SquirixCategory, SquirixLevel);
        _ = builder.AddConsole();
    }
}
