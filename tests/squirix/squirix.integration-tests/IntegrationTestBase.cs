using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using JetBrains.Annotations;
using Xunit;

namespace Squirix.IntegrationTests;

/// <summary>
/// Base class for squirix integration tests.
/// Provides helpers for starting nodes, building entries,
/// and creating test-scoped persistence directories.
/// </summary>
[SuppressMessage("Design", "CA1515", Justification = "xUnit test classes are public, so their shared base class must be at least as visible.")]
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
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Releases managed resources for derived classes.
    /// </summary>
    /// <param name="disposing">Whether managed resources should be released.</param>
    [PublicAPI]
    protected virtual void Dispose(bool disposing)
    {
    }
}
