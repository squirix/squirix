using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Provides a common base for server unit tests.</summary>
[Immutable]
public abstract class ServerUnitTestBase
{
    static ServerUnitTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", NodePathKit.GetProcTempPath());
    }

    /// <summary>
    /// Gets a default <see cref="CancellationToken" /> with a 30s timeout.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;
}
