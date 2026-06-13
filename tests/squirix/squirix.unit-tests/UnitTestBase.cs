using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using JetBrains.Annotations;
using Squirix.TestKit.IO;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Provides a common base for unit tests, offering a default
/// <see cref="CancellationToken" /> with a 30s timeout and safe disposal.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
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
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases managed resources for derived classes.
    /// </summary>
    /// <param name="disposing">Whether managed resources should be released.</param>
    [UsedImplicitly]
    protected virtual void Dispose(bool disposing)
    {
    }
}
