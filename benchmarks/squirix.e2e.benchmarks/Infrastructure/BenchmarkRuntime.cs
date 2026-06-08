using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Squirix.Server.TestKit.Http;

namespace Squirix.E2EBenchmarks.Infrastructure;

/// <summary>
/// Process-wide transport settings for end-to-end benchmarks against local HTTPS gRPC nodes.
/// </summary>
internal static class BenchmarkRuntime
{
    private static int _initialized;

    internal static void EnsureInitialized()
    {
        if (Interlocked.Exchange(ref _initialized, 1) == 1)
            return;

        LoopbackHttp.DisableSystemProxyForLocalTests();
    }

    internal static void ConfigureRemoteClient(SquirixOptions options)
    {
        EnsureInitialized();
        ArgumentNullException.ThrowIfNull(options);
        options.HttpMessageHandler ??= LoopbackHttp.CreateHandler();
    }

    [ModuleInitializer]
    internal static void ModuleInitialize() => EnsureInitialized();
}
