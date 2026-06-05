using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Process-wide transport settings for remote client benchmarks against local HTTP/2 gRPC nodes.
/// </summary>
internal static class BenchmarkRuntime
{
    private static int _initialized;

    internal static void EnsureInitialized()
    {
        if (Interlocked.Exchange(ref _initialized, 1) == 1)
            return;

        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT", "true");
    }

    internal static void ConfigureRemoteClient(SquirixOptions options)
    {
        EnsureInitialized();
        ArgumentNullException.ThrowIfNull(options);
    }

    [ModuleInitializer]
    internal static void ModuleInitialize() => EnsureInitialized();
}
