using System;
using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint.Rest;
using Squirix.Server.Contracts;
using Squirix.Server.Limits;
using Squirix.Server.Runtime.Contracts;
using static Squirix.Server.Adapters.Rest.RestDtos;

namespace Squirix.Server.Adapters.Rest;

/// <summary>
/// Defines REST and health endpoints exposed by squirix.
/// </summary>
internal static class HttpEndpointEx
{
    extension(IEndpointRouteBuilder app)
    {
        public void MapCacheEndpoints<T>(string routePrefix, bool requireAuth = false)
        {
            if (string.IsNullOrWhiteSpace(routePrefix))
                throw new ArgumentException("Route prefix must be non-empty.", nameof(routePrefix));

            // Per-request JSON/body size cap (bytes). Unrelated to in-memory scan limits, which bound key count, not payload bytes.
            var api = app.MapGroup("/api/v1");
            var cache = api.MapGroup(routePrefix).WithTags($"Cache<{typeof(T).Name}>");

            if (requireAuth)
                _ = cache.RequireAuthorization("ApiOrJwt");

            IEndpointRouteBuilder.MapCacheKeyValueEndpoints<T>(cache);
        }

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

        private static void MapCacheKeyValueEndpoints<T>(RouteGroupBuilder cache)
        {
            _ = cache.MapPut(
                "/{key}",
                static async (
                    string key,
                    CacheEntry<T> entry,
                    ICacheApi<T> apiSvc,
                    HttpRequest http,
                    HttpResponse resp,
                    IOptions<JsonOptions> jsonOptions,
                    CancellationToken cancellationToken) =>
                {
                    if (HttpEndpoints.ValidateKey(key) is { } keyError)
                        return keyError;
                    if (HttpEndpoints.ValidateContentLength(http, SquirixEntryLimits.MaxEntrySizeBytes) is { } lengthError)
                        return lengthError;
                    if (HttpEndpoints.ValidateEntry(entry, jsonOptions.Value.SerializerOptions, SquirixEntryLimits.MaxEntrySizeBytes) is { } entryError)
                        return entryError;

                    var existed = await apiSvc.GetEntryAsync(key, cancellationToken).ConfigureAwait(false) is not null;
                    await apiSvc.InsertAsync(key, entry, cancellationToken).ConfigureAwait(false);
                    var current = await apiSvc.GetEntryAsync(key, cancellationToken).ConfigureAwait(false);
                    if (current?.Version is { } version)
                        resp.Headers.ETag = $"\"{version}\"";

                    return existed ? Results.Ok() : Results.StatusCode(StatusCodes.Status201Created);
                });

            _ = cache.MapGet(
                "/{key}",
                static async (string key, ICacheApi<T> apiSvc, HttpResponse resp, CancellationToken cancellationToken) =>
                {
                    if (HttpEndpoints.ValidateKey(key) is { } keyError)
                        return keyError;

                    var e = await apiSvc.GetEntryAsync(key, cancellationToken);
                    if (e is null)
                        return CacheContractHttpResults.NotFound();

                    resp.Headers.ETag = $"\"{e.Version}\"";
                    return Results.Json(e);
                });

            _ = cache.MapMethods(
                "/{key}",
                ["HEAD"],
                static async (string key, ICacheApi<T> apiSvc, HttpRequest _, CancellationToken cancellationToken) =>
                {
                    if (HttpEndpoints.ValidateKey(key) is { } keyError)
                        return keyError;

                    var exists = await apiSvc.ContainsAsync(key, cancellationToken);
                    return exists ? Results.Ok() : Results.NotFound();
                });

            _ = cache.MapDelete(
                "/{key}",
                static async (string key, ICacheApi<T> apiSvc, HttpRequest _, CancellationToken cancellationToken) =>
                {
                    if (HttpEndpoints.ValidateKey(key) is { } keyError)
                        return keyError;

                    var ok = await apiSvc.RemoveAsync(key, cancellationToken);
                    return ok ? Results.NoContent() : CacheContractHttpResults.NotFound();
                });
        }
    }
}
