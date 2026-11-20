using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal sealed class ServerLifetime : INodeLifetime, IDisposable
{
    private readonly CancellationTokenSource _started = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly ILogger<ServerLifetime> _logger;

    internal ServerLifetime(ILogger<ServerLifetime> logger)
    {
        _logger = logger;
    }

    public CancellationToken ApplicationStarted => _started.Token;

    public CancellationToken ApplicationStopping => _stopping.Token;

    public void StopApplication()
    {
#pragma warning disable MA0045 // StopApplication synchronously notifies callers and captures callback failures.
        var failure = Isolated.Run(_stopping, static source => source.Cancel());
#pragma warning restore MA0045
        if (failure != null)
            _ = Isolated.Run((Logger: _logger, Error: failure), static state => DiagnosticsLog.LifetimeNotificationFailed(state.Logger, state.Error));
    }

    public void Dispose()
    {
        _stopping.Dispose();
        _started.Dispose();
    }

    internal async Task StartedAsync()
    {
        try
        {
            await _started.CancelAsync().ConfigureAwait(false);
        }
        catch (AggregateException failure)
        {
            _ = Isolated.Run((Logger: _logger, Error: failure), static state => DiagnosticsLog.LifetimeNotificationFailed(state.Logger, state.Error));
        }
    }
}
