using System;
using System.Threading;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Provides a common base for unit tests, offering a default
/// <see cref="CancellationToken" /> with a 30s timeout and safe disposal.
/// </summary>
public abstract class UnitTestBase : IDisposable
{
    static UnitTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", PathKit.GetProcTempPath());
    }

    /// <summary>
    /// Gets a default <see cref="CancellationToken" /> with a 30s timeout.
    /// Lazily created and reused per test instance.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;

    /// <summary>
    /// Cleans up sockets handler, HTTP client, and cancellation tokens.
    /// </summary>
    public virtual void Dispose() => GC.SuppressFinalize(this);
}
