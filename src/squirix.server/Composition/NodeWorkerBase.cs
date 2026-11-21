using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Composition;

/// <summary>Runs one background loop and requests its cancellation when the node stops.</summary>
internal abstract class NodeWorkerBase : INodeService, IDisposable
{
    private CancellationTokenSource? _stopping;

    internal Task? ExecuteTask { get; private set; }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var stopping = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        _stopping = stopping;
        var token = stopping.Token;
        ExecuteTask = RunAsync(token);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_stopping == null || ExecuteTask == null)
            return;

        await _stopping.CancelAsync().ConfigureAwait(false);
        await ExecuteTask.WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    protected virtual void Dispose(bool disposing)
    {
        if (disposing && _stopping != null)
        {
#pragma warning disable MA0045 // IDisposable must cancel the worker synchronously before releasing the token source.
            _stopping.Cancel();
#pragma warning restore MA0045
            _stopping.Dispose();
            _stopping = null;
        }
    }

    protected abstract Task ExecuteAsync(CancellationToken stoppingToken);

    private async Task RunAsync(CancellationToken token)
    {
        await Task.CompletedTask.ConfigureAwait(ConfigureAwaitOptions.ForceYielding);
        token.ThrowIfCancellationRequested();
        await ExecuteAsync(token).ConfigureAwait(false);
    }
}
