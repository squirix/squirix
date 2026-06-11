using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Squirix.Server.Adapters.Endpoint.Rest;
using Squirix.Server.Runtime.Contracts;
using static Squirix.Server.Adapters.Rest.RestDtos;

namespace Squirix.Server.Adapters.Rest;

/// <summary>
/// Defines health endpoints exposed by squirix.
/// </summary>
internal static class HttpEndpointEx
{
    extension(IEndpointRouteBuilder app)
    {
        public void MapHealthEndpoints()
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
            _ = app.MapGet(
                "/health/ready/details",
                static (IHealthReadyDetailsProvider provider) =>
                {
                    var snapshot = provider.GetSnapshot();
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

                    return Results.Json(
                        new HealthReadyDetailsResponse(
                            snapshot.JournalBacklogOps,
                            snapshot.SnapshotAgeSeconds,
                            snapshot.SnapshotInFlight,
                            compaction,
                            clientPool,
                            coordination,
                            memoryPressure),
                        RestJsonSerializerContext.Default.HealthReadyDetailsResponse);
                });
        }
    }
}
