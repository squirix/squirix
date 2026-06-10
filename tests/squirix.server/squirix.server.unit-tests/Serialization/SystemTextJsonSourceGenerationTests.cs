using System;
using System.Collections.Generic;
using System.Text.Json;
using Google.Protobuf;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.Storage.Snapshot;
using Xunit;
using static Squirix.Server.Adapters.Rest.RestDtos;
using RestJsonSerializerContext = Squirix.Server.Adapters.Endpoint.Rest.RestJsonSerializerContext;
using SquirixJsonSerializerContext = Squirix.Server.Serialization.SquirixJsonSerializerContext;

namespace Squirix.Server.UnitTests.Serialization;

/// <summary>
/// Tests for System.Text.Json source-generated metadata used by the default serializer.
/// </summary>
public sealed class SystemTextJsonSourceGenerationTests : ServerUnitTestBase
{
    /// <summary>
    /// Ensures the compact remove-expiration journal operation uses the persisted camelCase shape and round-trips.
    /// </summary>
    [Fact]
    public void JournalCodecRoundTripsRemoveExpiration()
    {
        var envelope = new JournalEnvelope
        {
            Seq = 22,
            UnixMs = 789,
            RemoveExpiration = new RemoveExpiration { Key = "k1", Namespace = "default" },
        };

        var bytes = RecordCodec.Serialize(envelope);
        using var document = JsonDocument.Parse(bytes);
        var root = document.RootElement;

        Assert.True(root.TryGetProperty("removeExpiration", out var removeExpiration));
        Assert.Equal("k1", removeExpiration.GetProperty("key").GetString());
        Assert.Equal("default", removeExpiration.GetProperty("namespace").GetString());

        var roundTrip = RecordCodec.Deserialize(bytes);

        Assert.Equal(JournalEnvelope.OpOneofCase.RemoveExpiration, roundTrip.OpCase);
        Assert.Equal("k1", roundTrip.RemoveExpiration.Key);
        Assert.Equal("default", roundTrip.RemoveExpiration.Namespace);
    }

    /// <summary>
    /// Ensures the compact touch-expiration journal operation uses the persisted camelCase shape and round-trips.
    /// </summary>
    [Fact]
    public void JournalCodecRoundTripsTouchExpiration()
    {
        var envelope = new JournalEnvelope
        {
            Seq = 23,
            UnixMs = 790,
            TouchExpiration = new TouchExpiration { Key = "k2", Namespace = "default", ExpiresUnixMs = 1_765_000_000_000 },
        };

        var bytes = RecordCodec.Serialize(envelope);
        using var document = JsonDocument.Parse(bytes);
        var root = document.RootElement;

        Assert.True(root.TryGetProperty("touchExpiration", out var touchExpiration));
        Assert.Equal("k2", touchExpiration.GetProperty("key").GetString());
        Assert.Equal("default", touchExpiration.GetProperty("namespace").GetString());
        Assert.Equal(1_765_000_000_000, touchExpiration.GetProperty("expiresUnixMs").GetInt64());

        var roundTrip = RecordCodec.Deserialize(bytes);

        Assert.Equal(JournalEnvelope.OpOneofCase.TouchExpiration, roundTrip.OpCase);
        Assert.Equal("k2", roundTrip.TouchExpiration.Key);
        Assert.Equal("default", roundTrip.TouchExpiration.Namespace);
        Assert.Equal(1_765_000_000_000, roundTrip.TouchExpiration.ExpiresUnixMs);
    }

    /// <summary>
    /// Ensures the runtime journal codec preserves the existing JSON envelope shape.
    /// </summary>
    [Fact]
    public void JournalCodecUsesGeneratedJsonContract()
    {
        var envelope = new JournalEnvelope
        {
            Seq = 21,
            UnixMs = 456,
            Put = new Put
            {
                OperationId = "op-codec",
                Item = new EntryPair
                {
                    Key = "journal-key",
                    EntryJson = ByteString.CopyFromUtf8("{\"value\":1}"),
                },
            },
        };

        var bytes = RecordCodec.Serialize(envelope);
        using var document = JsonDocument.Parse(bytes);
        var root = document.RootElement;

        Assert.True(root.TryGetProperty("seq", out var seq));
        Assert.Equal(21UL, seq.GetUInt64());
        Assert.True(root.TryGetProperty("unixMs", out _));
        Assert.True(root.TryGetProperty("put", out var put));
        Assert.True(put.TryGetProperty("operationId", out var operationId));
        Assert.Equal("op-codec", operationId.GetString());
        Assert.False(root.TryGetProperty("Seq", out _));
    }

    /// <summary>
    /// Ensures journal JSON DTOs keep the existing web/camelCase JSON contract.
    /// </summary>
    [Fact]
    public void JournalDtoUsesSourceGeneratedWebContract()
    {
        var serializer = new SystemTextJsonSerializer();
        var envelope = new RecordEnvelope
        {
            Seq = 12,
            UnixMs = 345,
            Put = new PutOp
            {
                OperationId = "op-1",
                Item = new ItemPair { Key = "k1", EntryJsonUtf8 = [.. "{\"v\":1}"u8] },
            },
        };

        var bytes = serializer.SerializeToUtf8Bytes(envelope);
        using var document = JsonDocument.Parse(bytes);
        var root = document.RootElement;

        Assert.True(root.TryGetProperty("seq", out var seq));
        Assert.Equal(12UL, seq.GetUInt64());
        Assert.True(root.TryGetProperty("unixMs", out _));
        Assert.True(root.TryGetProperty("put", out var put));
        Assert.True(put.TryGetProperty("operationId", out _));
        Assert.False(root.TryGetProperty("Seq", out _));

        var roundTrip = serializer.Deserialize<RecordEnvelope>(bytes);

        Assert.NotNull(roundTrip);
        Assert.Equal(12UL, roundTrip.Seq);
        Assert.Equal("op-1", roundTrip.Put?.OperationId);
        Assert.Equal("k1", roundTrip.Put?.Item.Key);
    }

    /// <summary>
    /// Ensures reflection fallback remains available for application payload types.
    /// </summary>
    [Fact]
    public void KeepsReflectionFallbackForUnknownApplicationTypes()
    {
        var serializer = new SystemTextJsonSerializer();
        var payload = serializer.SerializeToUtf8Bytes(new Dictionary<string, int> { ["value"] = 42 });

        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(payload);

        Assert.NotNull(roundTrip);
        Assert.Equal(42, roundTrip["value"]);
    }

    /// <summary>
    /// Ensures manifest serialization keeps the persisted camelCase property names.
    /// </summary>
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

    /// <summary>
    /// Ensures persistence DTOs outside journal are covered by the generated context.
    /// </summary>
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

    /// <summary>
    /// Ensures health diagnostics DTOs keep stable nested JSON shapes.
    /// </summary>
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
            new HealthMemoryPressureDetails("normal", 1024, 128, 3, 0, false));
        var healthElement = JsonSerializer.SerializeToElement(health, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);

        Assert.True(healthElement.TryGetProperty("journalBacklogOps", out var backlog));
        Assert.Equal(7, backlog.GetInt64());
        Assert.True(healthElement.TryGetProperty("memoryPressure", out var memoryPressure));
        Assert.True(memoryPressure.TryGetProperty("estimatedCacheBytes", out _));
        Assert.False(healthElement.TryGetProperty("JournalBacklogOps", out _));
    }

    /// <summary>
    /// Ensures REST response DTOs keep the public web JSON contract.
    /// </summary>
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

    /// <summary>
    /// Ensures SerializeToElement can still round-trip application payloads through reflection fallback.
    /// </summary>
    [Fact]
    public void SerializeToElementKeepsReflectionFallbackForUnknownApplicationTypes()
    {
        var serializer = new SystemTextJsonSerializer();
        var payload = new Dictionary<string, int> { ["value"] = 42 };

        var element = serializer.SerializeToElement(payload);
        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(element);

        Assert.NotNull(roundTrip);
        Assert.Equal(42, roundTrip["value"]);
    }

    /// <summary>
    /// Ensures SerializeToElement preserves the generated JSON contract for known DTOs.
    /// </summary>
    [Fact]
    public void SerializeToElementUsesConfiguredJsonContract()
    {
        var serializer = new SystemTextJsonSerializer();
        var envelope = new RecordEnvelope
        {
            Seq = 7,
            UnixMs = 999,
            Put = new PutOp
            {
                OperationId = "op-serialize-element",
                Item = new ItemPair { Key = "key-1", EntryJsonUtf8 = [.. "{\"value\":1}"u8] },
            },
        };

        var element = serializer.SerializeToElement(envelope);

        Assert.Equal(JsonValueKind.Object, element.ValueKind);
        Assert.True(element.TryGetProperty("seq", out var seq));
        Assert.Equal(7UL, seq.GetUInt64());
        Assert.True(element.TryGetProperty("put", out var put));
        Assert.True(put.TryGetProperty("operationId", out var operationId));
        Assert.Equal("op-serialize-element", operationId.GetString());
        Assert.False(element.TryGetProperty("Seq", out _));
    }

    /// <summary>
    /// Ensures snapshot metadata frames keep the persisted camelCase property names.
    /// </summary>
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
