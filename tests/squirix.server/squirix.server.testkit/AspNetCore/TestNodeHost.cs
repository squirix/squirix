using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Lightweight wrapper that hosts an ASP.NET Core <see cref="WebApplication" /> for integration tests.
/// Provides access to the service provider, test binding address, and temporary data directory,
/// and disposes the underlying app when the host is disposed.
/// </summary>
/// <remarks>
/// The instance owns the lifetime of the supplied <see cref="WebApplication" /> and will dispose it via
/// <see cref="DisposeAsync" />. Use this type to simplify test setup/teardown of an in-process Squirix node.
/// </remarks>
public sealed class TestNodeHost : IAsyncDisposable
{
    private readonly WebApplication _app;
    private readonly IDisposable? _scope;
    private int _disposed;

    /// <summary>
    /// Initializes a new instance of the <see cref="TestNodeHost" /> class.
    /// </summary>
    /// <param name="app">The preconfigured <see cref="WebApplication" /> to run inside the test host.</param>
    /// <param name="address">The listening address (scheme/host/port) used by the test node.</param>
    /// <param name="dataDir">Path to the data directory used by the test node (journal, snapshots, etc.).</param>
    /// <param name="scope">Optional disposable scope that will be disposed alongside the host.</param>
    public TestNodeHost(WebApplication app, string address, string dataDir, IDisposable? scope = null)
    {
        _app = app;
        Address = address;
        DataDir = dataDir;
        _scope = scope;
    }

    /// <summary>
    /// Gets the HTTP(S) address where the test node is reachable (e.g., <c>https://localhost:9443</c>).
    /// </summary>
    public string Address { get; }

    /// <summary>
    /// Gets the absolute path to the node's data directory created for the test run.
    /// </summary>
    public string DataDir { get; }

    /// <summary>
    /// Gets the root service provider of the hosted application for resolving test dependencies.
    /// </summary>
    public IServiceProvider Services => _app.Services;

    /// <summary>
    /// Asynchronously disposes the underlying <see cref="WebApplication" /> and releases resources.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that completes when disposal finishes.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        await SuppressObjectDisposedAsync(() => new ValueTask(_app.StopAsync(CancellationToken.None))).ConfigureAwait(false);
        await SuppressObjectDisposedAsync(() => _app.DisposeAsync()).ConfigureAwait(false);

        try
        {
            await JournalSegmentLeaseWait.WaitForReleasedAsync(DataDir, CancellationToken.None).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // Best-effort: another teardown path may already have removed or released the segment files.
        }

        _scope?.Dispose();
    }

    private static async ValueTask SuppressObjectDisposedAsync(Func<ValueTask> action)
    {
        try
        {
            await action().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // Best-effort teardown during test host shutdown.
        }
    }
}
