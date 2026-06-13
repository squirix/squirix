using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Provides a common base for server unit tests.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
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
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Disposes managed resources owned by the unit test base.
    /// </summary>
    /// <param name="disposing">True when called from <see cref="Dispose()" />; false from a finalizer path.</param>
    protected virtual void Dispose(bool disposing)
    {
    }
}
