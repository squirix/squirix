using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Diagnostics;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Lightweight wrapper that hosts an ASP.NET Core <see cref="WebApplication" /> for integration tests.
/// Provides access to the service provider, test binding address, and temporary data directory,
/// and disposes the underlying app when the host is disposed.
/// </summary>
/// <remarks>
/// The instance owns the lifetime of the supplied <see cref="WebApplication" /> and will shut it down via
/// <see cref="ITestNodeHost.ShutdownAsync" />. Use this type to simplify test setup/teardown of an in-process Squirix node.
/// </remarks>
[Immutable]
internal sealed class TestNodeHost : ITestNodeHost
{
    private readonly WebApplication _app;
    private readonly string _dataDir;
    private readonly bool _persistenceEnabled;
    private readonly IDisposable? _scope;
    private readonly Uri _uri;
    private int _disposed;
    private int _scopeDisposed;

    /// <summary>Initializes a new instance of the <see cref="TestNodeHost" /> class.</summary>
    /// <param name="app">The preconfigured <see cref="WebApplication" /> to run inside the test host.</param>
    /// <param name="uri">The listening address (scheme/host/port) used by the test node.</param>
    /// <param name="dataDir">Path to the data directory used by the test node (journal, snapshots, etc.).</param>
    /// <param name="persistenceEnabled">Whether persistence is enabled for the hosted node.</param>
    /// <param name="scope">Optional disposable scope that will be disposed of alongside the host.</param>
    public TestNodeHost(WebApplication app, Uri uri, string dataDir, bool persistenceEnabled = false, IDisposable? scope = null)
    {
        _app = app;
        _uri = uri;
        _dataDir = dataDir;
        _persistenceEnabled = persistenceEnabled;
        _scope = scope;
    }

    string ITestNodeHost.DataDir => _dataDir;

    bool ITestNodeHost.HasInterNodeMtlsListener => _app.Services.GetService<MtlsCertificate>() is { Enabled: true };

    bool ITestNodeHost.PersistenceEnabled => _persistenceEnabled;

    IServiceProvider ITestNodeHost.Services => _app.Services;

    Uri ITestNodeHost.Uri => _uri;

    /// <summary>Simulates an unclean process termination (for example SIGKILL) by disposing the host without graceful shutdown.</summary>
    async ValueTask ITestNodeHost.AbruptShutdownAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        await SuppressObjectDisposedAsync(_app.DisposeAsync()).ConfigureAwait(false);
        DisposeScope();

        // Abrupt dispose can leave Windows handles on man-current / journal segments draining briefly.
        // Offline compact and restart paths open those files immediately; wait until they are shareable.
        await WaitForPersistenceReleaseBestEffortAsync().ConfigureAwait(false);
    }

    /// <summary>Asynchronously disposes the underlying <see cref="WebApplication" /> and releases resources.</summary>
    ValueTask IAsyncDisposable.DisposeAsync() => ShutdownCoreAsync();

    /// <inheritdoc />
    ValueTask ITestNodeHost.ShutdownAsync() => ShutdownCoreAsync();

    private static async ValueTask SuppressObjectDisposedAsync(ValueTask task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            TestLog.Suppressed("Suppressed ObjectDisposedException during test host teardown.", ex);
        }
    }

    private async ValueTask ShutdownCoreAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        try
        {
            await SuppressObjectDisposedAsync(StopAppAsync()).ConfigureAwait(false);
        }
        finally
        {
            await SuppressObjectDisposedAsync(_app.DisposeAsync()).ConfigureAwait(false);
            await WaitForPersistenceReleaseBestEffortAsync().ConfigureAwait(false);
            DisposeScope();
        }
    }

    private void DisposeScope()
    {
        if (Interlocked.Exchange(ref _scopeDisposed, 1) == 0)
            _scope?.Dispose();
    }

    private async ValueTask StopAppAsync()
    {
        using var stopCts = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await _app.StopAsync(stopCts.Token).ConfigureAwait(false);
    }

    private async ValueTask WaitForPersistenceReleaseBestEffortAsync()
    {
        if (!_persistenceEnabled || string.IsNullOrWhiteSpace(_dataDir))
            return;

        try
        {
            await JournalSegmentLeaseWait.WaitForReleasedAsync(_dataDir, CancellationToken.None).ConfigureAwait(false);
        }
        catch (TimeoutException ex)
        {
            TestLog.Suppressed("Journal lease release wait timed out during teardown; assuming already released.", ex);
        }
    }
}
