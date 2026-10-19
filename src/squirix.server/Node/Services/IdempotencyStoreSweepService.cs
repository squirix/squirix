using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Periodically sweeps expired idempotency records in addition to lazy per-access sweeps.</summary>
internal sealed class IdempotencyStoreSweepService : BackgroundService
{
    private readonly ILogger<IdempotencyStoreSweepService> _log;
    private readonly IdempotencyOptions _options;
    private readonly RpcMutationIdempotencyStore _store;
    private readonly TimeProvider _timeProvider;

    public IdempotencyStoreSweepService(RpcMutationIdempotencyStore store, IOptions<IdempotencyOptions> options, ILogger<IdempotencyStoreSweepService> log, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(log);
        _log = log;
        _options = options.Value;
        _timeProvider = timeProvider;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.BackgroundSweepInterval, _timeProvider);
        await using var stop = timer.DisposeOnCancel(stoppingToken).ConfigureAwait(false);
        while (await timer.WaitForNextTickAsync(CancellationToken.None).ConfigureAwait(false))
            _store.SweepExpired();

        if (stoppingToken.IsCancellationRequested)
            ServerLog.IdempotencySweepStopped(_log);
    }
}
