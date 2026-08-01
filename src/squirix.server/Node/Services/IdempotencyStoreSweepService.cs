using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.Services;

/// <summary>Periodically sweeps expired idempotency records in addition to lazy per-access sweeps.</summary>
internal sealed class IdempotencyStoreSweepService : BackgroundService
{
    private readonly IdempotencyOptions _options;
    private readonly RpcMutationIdempotencyStore _store;

    public IdempotencyStoreSweepService(RpcMutationIdempotencyStore store, IOptions<IdempotencyOptions> options)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        ArgumentNullException.ThrowIfNull(options);
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
        catch (OperationCanceledException)
        {
            // Expected on host stop/dispose; do not fault BackgroundService (StopHost).
        }
    }
}
