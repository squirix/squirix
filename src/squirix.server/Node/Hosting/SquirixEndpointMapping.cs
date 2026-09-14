using System.Globalization;
using System.Text.Json;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Runtime.Diagnostics;
using ReplicationServiceAdapter = Squirix.Server.Adapters.Grpc.Replication.SquirixReplicationServiceAdapter;

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
        var featureState = app.Services.GetRequiredService<FeatureState>();
        var cacheGrpc = app.MapGrpcService<SquirixServiceAdapter<object?>>();
        if (authEnabled)
            _ = cacheGrpc.RequireAuthorization(JwtBearerAuthorizationPolicies);

        if (!mtlsMaterial.Enabled || mtlsOptions.InternalListenPort <= 0)
            return app;

        // Per-app filter: a shared static array would be overwritten when multiple in-process nodes map endpoints.
        string[] internalHostFilter = [string.Create(CultureInfo.InvariantCulture, $"*:{mtlsOptions.InternalListenPort}")];
        _ = app.MapGrpcService<SquirixServiceAdapter<object?>>().RequireHost(internalHostFilter).AllowAnonymous();

        // Closed replication service: internal listener only, for foundation transport tests
        // and on network-replication-activated hosts.
        if (featureState.FoundationOnly || featureState.NetworkReplicationEnabled)
            _ = app.MapGrpcService<ReplicationServiceAdapter>().RequireHost(internalHostFilter).AllowAnonymous();

        return app;
    }

    private static HealthReadyDetailsResponse BuildReadyDetailsResponse(HealthReadyDetailsSnapshot snapshot)
    {
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
        return new HealthReadyDetailsResponse(
            snapshot.JournalBacklogOps,
            snapshot.SnapshotAgeSeconds,
            snapshot.SnapshotInFlight,
            new HealthReadyDetailSections(compaction, clientPool, coordination, memoryPressure, retentionCleanup, journalDisk));
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

        // Manual response writing (instead of Results.Ok): the MapGet overload taking Delegate
        // is trim-unsafe, while this RequestDelegate-shaped handler binds the AOT-clean overload.
        // The bytes below match Results.Ok("OK") exactly: 200 + application/json + "\"OK\"".
        _ = app.MapGet(
            "/health",
            static async ctx =>
            {
                ctx.Response.ContentType = "application/json; charset=utf-8";
                await ctx.Response.WriteAsync("\"OK\"", ctx.RequestAborted).ConfigureAwait(false);
            });
        MapReadyDetailsEndpoint(app);
    }

    private static void MapReadyDetailsEndpoint(IEndpointRouteBuilder app)
    {
        // RequestDelegate-shaped handler (instead of a Delegate with DI parameters): the MapGet
        // overload taking Delegate is trim-unsafe, while this shape binds the AOT-clean overload.
        // Services resolve explicitly and the DTO is written with source-generated metadata,
        // so the wire behavior matches the previous Results-based version exactly.
        _ = app.MapGet(
            "/health/ready/details",
            static async ctx =>
            {
                if (!ConnectionSecurity.IsRequestAuthorized(ctx))
                {
                    ctx.Response.StatusCode = StatusCodes.Status401Unauthorized;
                    return;
                }

                var provider = ctx.RequestServices.GetRequiredService<IHealthReadyDetailsProvider>();
                var snapshot = await provider.GetSnapshotAsync(ctx.RequestAborted).ConfigureAwait(false);
                var details = BuildReadyDetailsResponse(snapshot);

                ctx.Response.ContentType = "application/json; charset=utf-8";
                await JsonSerializer.SerializeAsync(ctx.Response.Body, details, RestJsonSerializerContext.Default.HealthReadyDetailsResponse, ctx.RequestAborted)
                                    .ConfigureAwait(false);
            });
    }
}
