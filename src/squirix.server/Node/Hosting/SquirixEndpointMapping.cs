using System.Globalization;
using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixEndpointMapping
{
    private static readonly string[] JwtBearerAuthorizationPolicies = [SquirixAuthorizationPolicies.JwtBearer];

    internal static WebApplication MapSquirixEndpoints(this WebApplication app, bool authEnabled)
    {
        MapHealthEndpoints(app);

        var metricsOptions = app.Services.GetRequiredService<IOptions<PrometheusMetricsEndpointOptions>>().Value;
        if (metricsOptions.Enabled)
            app.MapSquirixMetrics(metricsOptions.Path);

        var mtlsOptions = app.Services.GetRequiredService<MtlsOptions>();
        var mtlsMaterial = app.Services.GetRequiredService<MtlsCertificateMaterial>();
        var cacheGrpc = app.MapGrpcService<SquirixServiceAdapter<object?>>();
        if (authEnabled)
            _ = cacheGrpc.RequireAuthorization(JwtBearerAuthorizationPolicies);

        if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0)
            return app;

        // Per-app filter: a shared static array would be overwritten when multiple in-process nodes map endpoints.
        string[] internalHostFilter = [string.Create(CultureInfo.InvariantCulture, $"*:{mtlsOptions.InternalListenPort}")];
        _ = app.MapGrpcService<SquirixServiceAdapter<object?>>().RequireHost(internalHostFilter).AllowAnonymous();
        return app;
    }

    private static void MapHealthEndpoints(IEndpointRouteBuilder app)
    {
        _ = app.MapHealthChecks(
            "/health/live",
            new HealthCheckOptions
            {
                Predicate = static registration => registration.Tags.Contains("live"),
            });
        _ = app.MapHealthChecks(
            "/health/ready",
            new HealthCheckOptions
            {
                Predicate = static registration => registration.Tags.Contains("ready"),
            });
        _ = app.MapGet("/health", static () => Results.Ok("OK"));
        MapReadyDetailsEndpoint(app);
    }

    private static void MapReadyDetailsEndpoint(IEndpointRouteBuilder app)
    {
        _ = app.MapGet(
            "/health/ready/details",
            static async (HttpContext ctx, IHealthReadyDetailsProvider provider, CancellationToken cancellationToken) =>
            {
                if (!ConnectionSecurity.IsRequestAuthorized(ctx))
                    return Results.Unauthorized();

                var snapshot = await provider.GetSnapshotAsync(cancellationToken).ConfigureAwait(false);
                var compaction = new HealthCompactionDetails(snapshot.Compaction.State, snapshot.Compaction.LastRunUtc, snapshot.Compaction.InFlight);
                var clientPool = new HealthClientPoolDetails(snapshot.ClientPool.Enabled, snapshot.ClientPool.PeerCount);
                var coordination = new HealthCoordinationDetails(
                    new HealthLeaseDetails(
                        snapshot.Coordination.Lease.Enabled,
                        snapshot.Coordination.Lease.ActiveLeases,
                        snapshot.Coordination.Lease.PendingGrants,
                        snapshot.Coordination.Lease.PendingReleases),
                    new HealthWatchDetails(
                        snapshot.Coordination.Watch.Enabled,
                        snapshot.Coordination.Watch.ActiveWatches,
                        snapshot.Coordination.Watch.DroppedEvents,
                        snapshot.Coordination.Watch.BufferedEvents));
                var memoryPressure = new HealthMemoryPressureDetails(
                    snapshot.MemoryPressure.State,
                    snapshot.MemoryPressure.MaxEstimatedCacheBytes,
                    snapshot.MemoryPressure.EstimatedBytes,
                    snapshot.MemoryPressure.EntryCount,
                    snapshot.MemoryPressure.RejectedWriteCount,
                    snapshot.MemoryPressure.WriteRejectionActive);
                var journalDisk = new HealthJournalDiskDetails(
                    snapshot.JournalDisk.State,
                    snapshot.JournalDisk.MaxBytes,
                    snapshot.JournalDisk.UsedBytes,
                    snapshot.JournalDisk.HighWaterBytes,
                    snapshot.JournalDisk.WriteRejectionActive);
                var retentionCleanup = new HealthRetentionCleanupDetails(
                    snapshot.RetentionCleanup.Degraded,
                    snapshot.RetentionCleanup.ConsecutiveWriteFailures,
                    snapshot.RetentionCleanup.RecentFailureCount,
                    snapshot.RetentionCleanup.LastFailureUtc);

                return Results.Json(
                    new HealthReadyDetailsResponse(
                        snapshot.JournalBacklogOps,
                        snapshot.SnapshotAgeSeconds,
                        snapshot.SnapshotInFlight,
                        new HealthReadyDetailSections(compaction, clientPool, coordination, memoryPressure, retentionCleanup, journalDisk)),
                    RestJsonSerializerContext.Default.HealthReadyDetailsResponse);
            });
    }
}
