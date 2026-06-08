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
        LoopbackHttp.EnsureDevelopmentCertificateTrusted();
        LoopbackHttp.DisableSystemProxyForLocalTests();
        Environment.SetEnvironmentVariable("SQUIRIX_ADMIN_ENABLED", "true");
    }

    /// <summary>
    /// Gets the default cancellation token for the current test.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
