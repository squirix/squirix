using System;
using System.Threading;
using Squirix.Server.TestKit.Http;
using Xunit;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Base class for SDK end-to-end tests.
/// </summary>
public abstract class E2ETestBase
{
    static E2ETestBase()
    {
        LoopbackHttp.DisableSystemProxyForLocalTests();
        AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);
        Environment.SetEnvironmentVariable("DOTNET_SYSTEM_NET_HTTP_SOCKETSHTTPHANDLER_HTTP2UNENCRYPTEDSUPPORT", "true");
        Environment.SetEnvironmentVariable("SQUIRIX_ADMIN_ENABLED", "true");
    }

    /// <summary>
    /// Gets the default cancellation token for the current test.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
