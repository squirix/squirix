using System;
using System.Threading;
using JetBrains.Annotations;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Provides a common base for server unit tests.</summary>
public abstract class UnitTestBase : IDisposable
{
    static UnitTestBase()
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

    /// <summary>Disposes managed resources owned by the unit test base.</summary>
    /// <param name="disposing">True when called from <see cref="Dispose()" />; false from a finalizer path.</param>
    [UsedImplicitly]
    protected virtual void Dispose(bool disposing)
    {
    }
}
