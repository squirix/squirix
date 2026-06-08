using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Json;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Server.Adapters.Endpoint.Rest;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
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
        public void MapAdminEndpoints(IHostEnvironment environment, bool requireAuth = false)
        {
            if (!HttpEndpoints.ShouldExposeAdminEndpoints(environment, out var flagValue))
            {
                var loggerFactory = app.ServiceProvider.GetService<ILoggerFactory>();
                var logger = loggerFactory?.CreateLogger("Squirix.Admin");
                if (logger is not null)
                    HttpEndpoints.LogAdminEndpointsSkipped(logger, environment.EnvironmentName, flagValue ?? "unset");
                return;
            }

            var services = app.ServiceProvider;
            var auditSink = services.GetRequiredService<AdminAuditSink>();
            var resolvedLoggerFactory = services.GetRequiredService<ILoggerFactory>();

            var admin = app.MapGroup("/admin");
            _ = admin.AddEndpointFilter(new AdminAuditFilter(auditSink, resolvedLoggerFactory.CreateLogger<AdminAuditFilter>()));

            if (requireAuth)
                _ = admin.RequireAuthorization("ApiOrJwt");

            IEndpointRouteBuilder.MapAdminIdentityAndOwnershipEndpoints(admin);
            IEndpointRouteBuilder.MapAdminCompactionEndpoints(admin);
            IEndpointRouteBuilder.MapAdminRingAndMembershipEndpoints(admin);
            IEndpointRouteBuilder.MapAdminAuditAndRebalanceEndpoints(admin);
            IEndpointRouteBuilder.MapAdminStorageDiagnosticsEndpoints(admin);
            IEndpointRouteBuilder.MapAdminUnsupportedMutationEndpoints(admin);
        }

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

        private static void MapAdminAuditAndRebalanceEndpoints(RouteGroupBuilder admin)
        {
            _ = admin.MapGet(
                "/audit",
                static (AdminAuditSink sink) =>
                {
                    var snapshot = sink.GetSnapshot();
                    var count = Math.Min(50, snapshot.Count);
                    var events = new AdminAuditEvent[count];
                    for (var i = 0; i < count; i++)
                        events[i] = snapshot[i];

                    return Results.Json(new AdminAuditResponse(events), RestJsonSerializerContext.Default.AdminAuditResponse);
                });
            _ = admin.MapGet(
                "/rebalance/history",
                static (IAdminClusterDiagnostics diagnostics) =>
                {
                    var snapshot = diagnostics.GetRebalanceHistory();
                    var events = new AdminRebalanceHistoryEvent[snapshot.Events.Length];
                    for (var i = 0; i < snapshot.Events.Length; i++)
                    {
                        var item = snapshot.Events[i];
                        events[i] = new AdminRebalanceHistoryEvent(
                            item.Sequence,
                            item.TimestampUtc,
                            item.Action,
                            item.NodeId,
                            item.PreviousMembers,
                            item.CurrentMembers,
                            item.PreviousVirtualNodes,
                            item.CurrentVirtualNodes);
                    }

                    return Results.Json(new AdminRebalanceHistoryResponse(snapshot.Retention, events), RestJsonSerializerContext.Default.AdminRebalanceHistoryResponse);
                });
        }

        private static void MapAdminCompactionEndpoints(RouteGroupBuilder admin)
        {
            _ = admin.MapPost(
                "/compact",
                static async (IAdminJournalCompactionTrigger trigger, CancellationToken token) =>
                {
                    var started = await trigger.TryTriggerCompactionAsync(token).ConfigureAwait(false);
                    return started ? Results.Accepted() : Results.StatusCode(StatusCodes.Status409Conflict);
                });
        }

        private static void MapAdminIdentityAndOwnershipEndpoints(RouteGroupBuilder admin)
        {
            _ = admin.MapGet(
                "/whoami",
                static (INodeEndpointIdentity identity) => Results.Json(
                    new AdminWhoamiResponse(identity.NodeId, identity.Url),
                    RestJsonSerializerContext.Default.AdminWhoamiResponse));
            _ = admin.MapGet(
                "/owner/{key}",
                static (string key, INodeOwnershipResolver ownership) => Results.Json(
                    new AdminOwnerResponse(key, ownership.GetOwner(CacheNames.DefaultNamespace, key)),
                    RestJsonSerializerContext.Default.AdminOwnerResponse));
        }

        private static void MapAdminRingAndMembershipEndpoints(RouteGroupBuilder admin)
        {
            _ = admin.MapGet(
                "/ring",
                static (IAdminClusterDiagnostics diagnostics) =>
                {
                    const int sampleSize = 256;
                    var snapshot = diagnostics.GetRingDiagnostics(sampleSize);
                    var distribution = new List<AdminRingNodeDistribution>(snapshot.VnodeDistribution.Count);
                    foreach (var node in snapshot.VnodeDistribution)
                        distribution.Add(new AdminRingNodeDistribution(node.NodeId, node.SampleKeys, node.SampleShare, node.ConfiguredVirtualNodes));

                    var ownerLookupPreview = new List<AdminOwnerLookupSample>(snapshot.OwnerLookupSamples.Count);
                    foreach (var sample in snapshot.OwnerLookupSamples)
                        ownerLookupPreview.Add(new AdminOwnerLookupSample(sample.Key, sample.Owner));

                    return Results.Json(
                        new AdminRingResponse(snapshot.VirtualNodes, snapshot.Members, snapshot.SampleSize, distribution, ownerLookupPreview),
                        RestJsonSerializerContext.Default.AdminRingResponse);
                });

            _ = admin.MapGet(
                "/members",
                static (IAdminClusterDiagnostics diagnostics) =>
                {
                    var snapshot = diagnostics.GetMembersDiagnostics();
                    return Results.Json(new AdminMembersResponse(snapshot.Members, snapshot.VirtualNodes), RestJsonSerializerContext.Default.AdminMembersResponse);
                });
        }

        private static void MapAdminStorageDiagnosticsEndpoints(RouteGroupBuilder admin)
        {
            _ = admin.MapGet(
                "/diagnostics/storage",
                static (IAdminStorageDiagnostics diagnostics) =>
                {
                    const int maxSegments = 16;
                    var snapshot = diagnostics.GetSnapshot(maxSegments);
                    var segments = new AdminJournalSegmentDiagnostic[snapshot.Journal.Segments.Length];
                    for (var i = 0; i < snapshot.Journal.Segments.Length; i++)
                    {
                        var segment = snapshot.Journal.Segments[i];
                        segments[i] = new AdminJournalSegmentDiagnostic(
                            segment.Index,
                            segment.Path,
                            segment.FileName,
                            segment.Exists,
                            segment.LengthBytes,
                            segment.LastWriteUtc,
                            segment.HeaderValid,
                            segment.Error);
                    }

                    return Results.Json(
                        new AdminStorageDiagnosticsResponse(
                            snapshot.DataDir,
                            snapshot.Manifest,
                            new AdminJournalWriterDiagnostics(
                                snapshot.Writer.CurrentJournal,
                                snapshot.Writer.NextSequence,
                                snapshot.Writer.AppendedOps,
                                snapshot.Writer.AppendedBytes,
                                snapshot.Writer.RecentAppendLatencyMs),
                            new AdminJournalDiagnostics(snapshot.Journal.RecentSegmentLimit, segments)),
                        RestJsonSerializerContext.Default.AdminStorageDiagnosticsResponse);
                });
        }

        private static void MapAdminUnsupportedMutationEndpoints(RouteGroupBuilder admin)
        {
            _ = admin.MapPost("/join", static () => IEndpointRouteBuilder.RejectUnsupportedMembershipMutation());
            _ = admin.MapPost("/leave", static () => IEndpointRouteBuilder.RejectUnsupportedMembershipMutation());
            _ = admin.MapPost("/vnodes", static () => IEndpointRouteBuilder.RejectUnsupportedMembershipMutation());
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

        private static IResult RejectUnsupportedMembershipMutation()
        {
            return Results.Json(
                new AdminUnsupportedMutationResponse(
                    "Runtime membership mutation is not supported by core static routing."),
                RestJsonSerializerContext.Default.AdminUnsupportedMutationResponse,
                statusCode: StatusCodes.Status409Conflict);
        }
    }
}
