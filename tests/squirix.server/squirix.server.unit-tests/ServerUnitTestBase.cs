using System;
using System.Threading;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Provides a common base for server unit tests.
/// </summary>
public abstract class ServerUnitTestBase : IDisposable
{
    static ServerUnitTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", PathKit.GetProcTempPath());
    }

    /// <summary>
    /// Gets a default <see cref="CancellationToken" /> with a 30s timeout.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;

    /// <inheritdoc />
    public virtual void Dispose() => GC.SuppressFinalize(this);
}
