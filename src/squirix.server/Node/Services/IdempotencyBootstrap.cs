using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Loads idempotency store settings from environment variables.</summary>
internal static class IdempotencyBootstrap
{
    /// <summary>Loads idempotency settings with environment overrides applied.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Resolved idempotency options.</returns>
    internal static Task<IdempotencyOptions> LoadAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return Task.FromResult(ApplyEnvironment(new IdempotencyOptions()));
    }

    private static IdempotencyOptions ApplyEnvironment(IdempotencyOptions options)
    {
        var result = options;

        var maxRecords = EnvVariables.ReadInt("SQUIRIX_IDEMPOTENCY_MAX_IN_FLIGHT_RECORDS");
        if (maxRecords is not null)
            result = result with { MaxInFlightRecords = maxRecords.Value };

        var retentionMinutes = EnvVariables.ReadInt("SQUIRIX_IDEMPOTENCY_RETENTION_MINUTES");
        if (retentionMinutes is not null)
            result = result with { Retention = TimeSpan.FromMinutes(retentionMinutes.Value) };

        var sweepSeconds = EnvVariables.ReadInt("SQUIRIX_IDEMPOTENCY_SWEEP_INTERVAL_SECONDS");
        if (sweepSeconds is not null)
            result = result with { BackgroundSweepInterval = TimeSpan.FromSeconds(sweepSeconds.Value) };

        return result;
    }
}
