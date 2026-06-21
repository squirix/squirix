using System;
using System.Collections.Generic;
using System.Text.Json;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using Xunit;
using static Squirix.Server.Adapters.Rest.RestDtos;
using RestJsonSerializerContext = Squirix.Server.Adapters.Endpoint.Rest.RestJsonSerializerContext;
using SquirixJsonSerializerContext = Squirix.Server.Serialization.SquirixJsonSerializerContext;

namespace Squirix.Server.UnitTests.Serialization;

/// <summary>Tests for System.Text.Json source-generated metadata used by the default serializer.</summary>
public sealed class SystemTextJsonSourceGenerationTests : UnitTestBase
{
    /// <summary>Ensures reflection fallback remains available for application payload types.</summary>
    [Fact]
    public void KeepsReflectionFallbackForUnknownApplicationTypes()
    {
        var serializer = new SystemTextJsonSerializer();
        var payload = serializer.SerializeToUtf8Bytes(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["value"] = 42 });

        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(payload);

        Assert.NotNull(roundTrip);
        Assert.Equal(42, roundTrip["value"]);
    }

    /// <summary>Ensures manifest serialization keeps the persisted camelCase property names.</summary>
    [Fact]
    public void ManifestContextPreservesPersistedJsonShape()
    {
        var manifest = new Manifest
        {
            CurrentJournal = 5,
            NextSequence = 55,
            LastSnapshot = new Manifest.SnapshotRef
            {
                Index = 4,
                Path = "snapshots/snapshot-000004.jsonl",
                CreatedUtc = new DateTime(2026, 5, 1, 2, 3, 4, DateTimeKind.Utc),
                LastAppliedSequence = 54,
                ReplayFromJournalSegment = 3,
            },
        };

        var element = JsonSerializer.SerializeToElement(manifest, SquirixJsonSerializerContext.Default.Manifest);

        Assert.True(element.TryGetProperty("currentJournal", out var currentJournal));
        Assert.Equal(5, currentJournal.GetInt32());
        Assert.True(element.TryGetProperty("nextSequence", out _));
        Assert.True(element.TryGetProperty("lastSnapshot", out var snapshot));
        Assert.True(snapshot.TryGetProperty("replayFromJournalSegment", out var replayFromSnapshot));
        Assert.Equal(3, replayFromSnapshot.GetInt32());
        Assert.False(element.TryGetProperty("CurrentJournal", out _));
    }

    /// <summary>Ensures persistence DTOs outside journal are covered by the generated context.</summary>
    [Fact]
    public void PersistenceDtosRoundTripWithGeneratedMetadata()
    {
        var serializer = new SystemTextJsonSerializer();
        var manifest = new Manifest
        {
            CurrentJournal = 3,
            NextSequence = 42,
            LastSnapshot = new Manifest.SnapshotRef
            {
                Index = 2,
                Path = "snapshots/snapshot-000002.jsonl",
                CreatedUtc = new DateTime(2026, 4, 10, 1, 2, 3, DateTimeKind.Utc),
                LastAppliedSequence = 41,
                ReplayFromJournalSegment = 2,
            },
        };
        var snapshot = new SnapshotFrame
        {
            Kind = "idempotency",
            Idempotency = new PersistedIdempotencyRecord
            {
                OperationId = "op-2",
                Fingerprint = "fp",
                CreatedUtc = manifest.LastSnapshot.CreatedUtc,
                Outcome = new PersistedIdempotencyOutcome { Kind = "insert" },
            },
        };

        var manifestRoundTrip = serializer.Deserialize<Manifest>(serializer.SerializeToUtf8Bytes(manifest));
        var snapshotRoundTrip = serializer.Deserialize<SnapshotFrame>(serializer.SerializeToUtf8Bytes(snapshot));

        Assert.NotNull(manifestRoundTrip);
        Assert.Equal(3, manifestRoundTrip.CurrentJournal);
        Assert.Equal(2, manifestRoundTrip.LastSnapshot?.ReplayFromJournalSegment);

        Assert.NotNull(snapshotRoundTrip);
        Assert.Equal("idempotency", snapshotRoundTrip.Kind);
        Assert.Equal("op-2", snapshotRoundTrip.Idempotency?.OperationId);
        Assert.Equal("insert", snapshotRoundTrip.Idempotency?.Outcome.Kind);
    }

    /// <summary>Ensures health diagnostics DTOs keep stable nested JSON shapes.</summary>
    [Fact]
    public void RestContextPreservesHealthJsonShape()
    {
        var health = new HealthReadyDetailsResponse(
            7,
            12.5,
            true,
            new HealthCompactionDetails("idle", null, false),
            new HealthClientPoolDetails(true, 2),
            new HealthCoordinationDetails(new HealthLeaseDetails(false, 0, 0, 0), new HealthWatchDetails(false, 0, 0, 0)),
            new HealthMemoryPressureDetails("normal", 1024, 128, 3, 0, false),
            new HealthRetentionCleanupDetails(false, 0, 0, null));
        var healthElement = JsonSerializer.SerializeToElement(health, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);

        Assert.True(healthElement.TryGetProperty("journalBacklogOps", out var backlog));
        Assert.Equal(7UL, backlog.GetUInt64());
        Assert.True(healthElement.TryGetProperty("memoryPressure", out var memoryPressure));
        Assert.True(memoryPressure.TryGetProperty("estimatedCacheBytes", out _));
        Assert.True(healthElement.TryGetProperty("retentionCleanup", out var retentionCleanup));
        Assert.False(retentionCleanup.GetProperty("degraded").GetBoolean());
        Assert.False(healthElement.TryGetProperty("JournalBacklogOps", out _));
    }

    /// <summary>Ensures REST response DTOs keep the public web JSON contract.</summary>
    [Fact]
    public void RestContextPreservesPublicResponseJsonShape()
    {
        var element = JsonSerializer.SerializeToElement(new RestIncrementResponse(42), RestJsonSerializerContext.Default.RestIncrementResponse);

        Assert.True(element.TryGetProperty("value", out var value));
        Assert.Equal(42, value.GetInt64());
        Assert.False(element.TryGetProperty("Value", out _));

        var error = JsonSerializer.SerializeToElement(new RestErrorResponse("missing", "notFound", null), RestJsonSerializerContext.Default.RestErrorResponse);

        Assert.True(error.TryGetProperty("error", out _));
        Assert.True(error.TryGetProperty("code", out _));
        Assert.True(error.TryGetProperty("detail", out var detail));
        Assert.Equal(JsonValueKind.Null, detail.ValueKind);
    }

    /// <summary>Ensures SerializeToElement can still round-trip application payloads through reflection fallback.</summary>
    [Fact]
    public void SerializeToElementKeepsReflectionFallbackForUnknownApplicationTypes()
    {
        var serializer = new SystemTextJsonSerializer();
        var payload = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["value"] = 42 };

        var element = serializer.SerializeToElement(payload);
        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(element);

        Assert.NotNull(roundTrip);
        Assert.Equal(42, roundTrip["value"]);
    }

    /// <summary>Ensures snapshot metadata frames keep the persisted camelCase property names.</summary>
    [Fact]
    public void SnapshotFrameContextPreservesPersistedJsonShape()
    {
        var frame = new SnapshotFrame
        {
            Kind = "idempotency",
            Idempotency = new PersistedIdempotencyRecord
            {
                OperationId = "snapshot-op",
                Fingerprint = "fingerprint",
                CreatedUtc = new DateTime(2026, 5, 1, 2, 3, 4, DateTimeKind.Utc),
                Outcome = new PersistedIdempotencyOutcome { Kind = "insert" },
            },
        };

        var element = JsonSerializer.SerializeToElement(frame, SquirixJsonSerializerContext.Default.SnapshotFrame);

        Assert.True(element.TryGetProperty("kind", out var kind));
        Assert.Equal("idempotency", kind.GetString());
        Assert.True(element.TryGetProperty("idempotency", out var idempotency));
        Assert.True(idempotency.TryGetProperty("operationId", out var operationId));
        Assert.Equal("snapshot-op", operationId.GetString());
        Assert.True(idempotency.TryGetProperty("outcome", out var outcome));
        Assert.True(outcome.TryGetProperty("kind", out var outcomeKind));
        Assert.Equal("insert", outcomeKind.GetString());
        Assert.False(element.TryGetProperty("Kind", out _));
    }
}
