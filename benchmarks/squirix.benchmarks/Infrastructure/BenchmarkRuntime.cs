using System;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Process-wide transport settings for remote client benchmarks against local HTTPS gRPC nodes.
/// </summary>
internal static class BenchmarkRuntime
{
    internal static void ConfigureRemoteClient(SquirixOptions options) => ArgumentNullException.ThrowIfNull(options);

    internal static void EnsureInitialized()
    {
        // Reserved for process-wide benchmark runtime initialization.
    }
}
