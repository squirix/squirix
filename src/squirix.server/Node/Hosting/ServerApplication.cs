using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Runs node services in registration order and drains them before closing their storage.</summary>
internal sealed class ServerApplication : IAsyncDisposable
{
    private readonly NodeProvider _services;
    private readonly IDisposable? _configuration;
    private readonly ServerLifetime _lifetime;
    private readonly ILogger<ServerApplication> _logger;
    private readonly List<INodeService> _started = [];
    private readonly List<Task> _workers = [];
    private readonly List<PosixSignalRegistration> _signals = [];

    private ServerApplication(NodeProvider services, IDisposable? configuration, ServerLifetime lifetime, ILogger<ServerApplication> logger)
    {
        _services = services;
        _configuration = configuration;
        _lifetime = lifetime;
        _logger = logger;
    }

    internal IServiceProvider Services => _services;

    public async ValueTask DisposeAsync()
    {
        for (var index = 0; index < _signals.Count; index++)
            _signals[index].Dispose();

        try
        {
            await _services.DisposeAsync().ConfigureAwait(false);
        }
        finally
        {
            _configuration?.Dispose();
        }
    }

    internal static ServerApplication Build(NodeServices services, IDisposable? configuration = null)
    {
        _ = services.Own(static sp => new ServerLifetime(sp.Require<ILoggerFactory>().CreateLogger<ServerLifetime>()));
        _ = services.Own<INodeLifetime>(static provider => provider.Require<ServerLifetime>());
        var provider = new NodeProvider(services);
        return new ServerApplication(provider, configuration, provider.Require<ServerLifetime>(), provider.Require<ILoggerFactory>().CreateLogger<ServerApplication>());
    }

    internal async Task StartAsync(CancellationToken cancellationToken)
    {
        using var startup = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _lifetime.ApplicationStopping);
        foreach (var signal in new[] { PosixSignal.SIGINT, PosixSignal.SIGQUIT, PosixSignal.SIGTERM })
            _signals.Add(PosixSignalRegistration.Create(signal, OnSignal));

        foreach (var service in _services.All<INodeService>())
        {
            startup.Token.ThrowIfCancellationRequested();

            // A start can acquire resources and then throw. Include that service in rollback.
            _started.Add(service);
            await service.StartAsync(startup.Token).ConfigureAwait(false);
            if (service is NodeWorker worker)
                TrackWorker(worker);
        }

        startup.Token.ThrowIfCancellationRequested();
        await _lifetime.StartedAsync().ConfigureAwait(false);
    }

    internal async Task StopAsync(CancellationToken cancellationToken)
    {
        using var budget = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        budget.CancelAfter(TimeSpan.FromSeconds(30));
        var failures = new List<Exception>();
        _lifetime.StopApplication();
#pragma warning disable CA1031 // Each stop failure is collected and rethrown after the journal drains.
        for (var index = _started.Count - 1; index >= 0; index--)
        {
            try
            {
                await _started[index].StopAsync(budget.Token).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                failures.Add(ex);
            }
        }

        // The journal uses its own flush budget after the listeners and all writers have stopped.
        for (var index = _started.Count - 1; index >= 0; index--)
        {
            if (_started[index] is not JournalStopService lifecycle)
                continue;

            try
            {
                await lifecycle.DrainAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                failures.Add(ex);
            }
        }

#pragma warning restore CA1031
        await Task.WhenAll(_workers).WaitAsync(budget.Token).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        if (failures.Count > 0)
            throw new AggregateException(failures);
    }

    private void TrackWorker(NodeWorker worker)
    {
        if (worker.ExecuteTask != null)
            _workers.Add(ObserveWorkerAsync(worker, _lifetime.ApplicationStopping));
    }

    private async Task ObserveWorkerAsync(NodeWorker worker, CancellationToken stopping)
    {
        try
        {
#pragma warning disable VSTHRD003 // The background service owns its worker task. It runs on the thread pool.
            await worker.ExecuteTask!.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            // The service observed its shutdown token.
        }
#pragma warning disable CA1031 // A failed background worker must signal process shutdown.
        catch (Exception ex)
        {
            try
            {
                DiagnosticsLog.ServerWorkerFailed(_logger, worker.GetType().Name, ex);
            }
            finally
            {
                _lifetime.StopApplication();
            }
        }
    }

#pragma warning restore CA1031

    private void OnSignal(PosixSignalContext context)
    {
        context.Cancel = true;
        _lifetime.StopApplication();
    }
}
