using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.TestKit.Hosting;

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
    /// <param name="uri">The listening address (scheme/host/port) used by the test node.</param>
    /// <param name="dataDir">Path to the data directory used by the test node (journal, snapshots, etc.).</param>
    /// <param name="persistenceEnabled">Whether persistence is enabled for the hosted node.</param>
    /// <param name="scope">Optional disposable scope that will be disposed alongside the host.</param>
    public TestNodeHost(WebApplication app, Uri uri, string dataDir, bool persistenceEnabled = false, IDisposable? scope = null)
    {
        _app = app;
        Uri = uri;
        DataDir = dataDir;
        PersistenceEnabled = persistenceEnabled;
        _scope = scope;
    }

    /// <summary>
    /// Gets the HTTP(S) address where the test node is reachable (e.g., <c>https://localhost:9443</c>).
    /// </summary>
    public Uri Uri { get; }

    /// <summary>Gets the absolute path to the node's data directory created for the test run.</summary>
    public string DataDir { get; }

    /// <summary>Gets a value indicating whether persistence is enabled for the hosted node.</summary>
    public bool PersistenceEnabled { get; }

    /// <summary>Gets the root service provider of the hosted application for resolving test dependencies.</summary>
    public IServiceProvider Services => _app.Services;

    /// <summary>Simulates an unclean process termination (for example SIGKILL) by disposing the host without graceful shutdown.</summary>
    public async ValueTask AbruptShutdownAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        await SuppressObjectDisposedAsync(() => _app.DisposeAsync()).ConfigureAwait(false);
        _scope?.Dispose();
    }

    /// <summary>
    /// Asynchronously disposes the underlying <see cref="WebApplication" /> and releases resources.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        await SuppressObjectDisposedAsync(async () =>
        {
            using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
            await _app.StopAsync(stopCts.Token).ConfigureAwait(false);
        }).ConfigureAwait(false);
        await SuppressObjectDisposedAsync(() => _app.DisposeAsync()).ConfigureAwait(false);

        if (PersistenceEnabled && !string.IsNullOrWhiteSpace(DataDir))
        {
            try
            {
                await JournalSegmentLeaseWait.WaitForReleasedAsync(DataDir, CancellationToken.None).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                // Best-effort: another teardown path may already have removed or released the segment files.
            }
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
