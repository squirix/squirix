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
    private readonly IdempotencyOptions _options;
    private readonly ILogger<IdempotencyStoreSweepService> _log;
    private readonly RpcMutationIdempotencyStore _store;

    public IdempotencyStoreSweepService(RpcMutationIdempotencyStore store, IOptions<IdempotencyOptions> options, ILogger<IdempotencyStoreSweepService> log)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        ArgumentNullException.ThrowIfNull(options);
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _options = options.Value;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.BackgroundSweepInterval);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                _store.SweepExpired(DateTime.UtcNow);
        }
        catch (OperationCanceledException ex)
        {
            // Expected on host stop/dispose; do not fault BackgroundService (StopHost).
            LogManager.IdempotencySweepCanceled(_log, ex);
        }
    }
}
