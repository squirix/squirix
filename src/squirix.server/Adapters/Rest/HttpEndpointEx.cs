using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Runtime.Contracts;
using static Squirix.Server.Adapters.Rest.Dtos;

namespace Squirix.Server.Adapters.Rest;

/// <summary>Defines health endpoints exposed by squirix.</summary>
internal static class HttpEndpointEx
{
    extension(IEndpointRouteBuilder app)
    {
        internal void MapHealthEndpoints()
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
                        new HealthReadyDetailSections(compaction, clientPool, coordination, memoryPressure, retentionCleanup)),
                    RestJsonSerializerContext.Default.HealthReadyDetailsResponse);
            });
    }
}
