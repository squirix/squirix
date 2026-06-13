using System;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Process-wide transport settings for end-to-end benchmarks against local HTTPS gRPC nodes.
/// </summary>
internal static class BenchmarkRuntime
{
    internal static void ConfigureRemoteClient(SquirixOptions options) => ArgumentNullException.ThrowIfNull(options);

    internal static void EnsureInitialized()
    {
    }
}
