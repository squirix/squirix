using System;
using System.Threading;
using Xunit;

namespace Squirix.IntegrationTests;

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

    /// <summary>
    /// Cleans up sockets handler, HTTP client, and cancellation tokens.
    /// </summary>
    public virtual void Dispose() => GC.SuppressFinalize(this);
}
