using System;
using System.Threading;
using JetBrains.Annotations;
using Xunit;

namespace Squirix.IntegrationTests.Support;

/// <summary>
/// Base class for squirix integration tests.
/// Provides helpers for starting nodes, building entries,
/// and creating test-scoped persistence directories.
/// </summary>
public abstract class IntegrationTestBase : IDisposable
{
    /// <summary>
    /// Gets a default <see cref="CancellationToken" /> with a 30s timeout,
    /// recreated lazily on first access.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;

    /// <summary>Cleans up sockets handler, HTTP client, and cancellation tokens.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Releases managed resources for derived classes.</summary>
    /// <param name="disposing">Whether managed resources should be released.</param>
    [PublicAPI]
    protected virtual void Dispose(bool disposing)
    {
    }
}
