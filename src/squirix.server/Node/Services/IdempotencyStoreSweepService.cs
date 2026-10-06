using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Periodically sweeps expired idempotency records in addition to lazy per-access sweeps.</summary>
internal sealed class IdempotencyStoreSweepService : BackgroundService
{
    private readonly ILogger<IdempotencyStoreSweepService> _log;
    private readonly IdempotencyOptions _options;
    private readonly RpcMutationIdempotencyStore _store;
    private readonly TimeProvider _timeProvider;

    public IdempotencyStoreSweepService(RpcMutationIdempotencyStore store, IOptions<IdempotencyOptions> options, ILogger<IdempotencyStoreSweepService> log, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(log);
        _log = log;
        _options = options.Value;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.BackgroundSweepInterval, _timeProvider);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                _store.SweepExpired();
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            ServerLog.IdempotencySweepStopped(_log);
        }
    }
}
