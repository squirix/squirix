using System;
using System.Runtime.CompilerServices;
using System.Threading;
using Squirix.Server.TestKit.Http;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Process-wide transport settings for remote client benchmarks against local HTTPS gRPC nodes.
/// </summary>
internal static class BenchmarkRuntime
{
    private static int _initialized;

    internal static void EnsureInitialized()
    {
        if (Interlocked.Exchange(ref _initialized, 1) == 1)
            return;

        LoopbackHttp.EnsureDevelopmentCertificateTrusted();
    }

    internal static void ConfigureRemoteClient(SquirixOptions options)
    {
        EnsureInitialized();
        ArgumentNullException.ThrowIfNull(options);
    }

    [ModuleInitializer]
    internal static void ModuleInitialize() => EnsureInitialized();
}
