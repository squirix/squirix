using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for System.Text.Json source-generated metadata used by the default serializer.</summary>
[Immutable]
public sealed class ServerJsonSerializerTests : ServerUnitTestBase
{
    /// <summary>Ensures reflection fallback remains available for application payload types.</summary>
    [Test]
    public async Task ReflectionFallbackForUnknownTypes()
    {
        var serializer = new ServerJsonSerializer();
        var payload = serializer.SerializeToUtf8Bytes(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["value"] = 42 });

        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(payload);

        _ = await Assert.That(roundTrip).IsNotNull();
        _ = await Assert.That(roundTrip["value"]).IsEqualTo(42);
    }

    /// <summary>Ensures REST error DTOs keep the public web JSON contract.</summary>
    [Test]
    public async Task RestContextPreservesErrorJsonShape()
    {
        var error = JsonSerializer.SerializeToElement(new ErrorResponse("missing", "notFound", null), RestJsonSerializerContext.Default.ErrorResponse);

        _ = await Assert.That(error.TryGetProperty("error", out _)).IsTrue();
        _ = await Assert.That(error.TryGetProperty("code", out _)).IsTrue();
        _ = await Assert.That(error.TryGetProperty("detail", out var detail)).IsTrue();
        _ = await Assert.That(detail.ValueKind).IsEqualTo(JsonValueKind.Null);
    }

    /// <summary>Ensures health diagnostics DTOs keep stable nested JSON shapes.</summary>
    [Test]
    public async Task RestContextPreservesHealthJsonShape()
    {
        var journalDiskDetails = new HealthJournalDiskDetails("normal", 2048L * 1024 * 1024, 128, 1638L * 1024 * 1024, false);
        var health = new HealthReadyDetailsResponse(
            7,
            12.5,
            true,
            new HealthReadyDetailSections(
                new HealthCompactionDetails("idle", null, false),
                new HealthClientPoolDetails(true, 2),
                new HealthCoordinationDetails(new HealthLeaseDetails(false, 0, 0, 0), new HealthWatchDetails(false, 0, 0, 0)),
                new HealthMemoryPressureDetails("normal", 1024, 128, 3, 0, false),
                new HealthRetentionCleanupDetails(false, 0, 0, null),
                journalDiskDetails));
        var healthElement = JsonSerializer.SerializeToElement(health, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);

        _ = await Assert.That(healthElement.TryGetProperty("journalBacklogOps", out var backlog)).IsTrue();
        _ = await Assert.That(backlog.GetUInt64()).IsEqualTo(7UL);
        _ = await Assert.That(healthElement.TryGetProperty("memoryPressure", out var memoryPressure)).IsTrue();
        _ = await Assert.That(memoryPressure.TryGetProperty("estimatedCacheBytes", out _)).IsTrue();
        _ = await Assert.That(healthElement.TryGetProperty("journalDisk", out var journalDisk)).IsTrue();
        _ = await Assert.That(journalDisk.GetProperty("state").GetString()).IsEqualTo(journalDiskDetails.State);
        _ = await Assert.That(journalDisk.GetProperty("maxBytes").GetInt64()).IsEqualTo(journalDiskDetails.MaxBytes);
        _ = await Assert.That(journalDisk.GetProperty("usedBytes").GetInt64()).IsEqualTo(journalDiskDetails.UsedBytes);
        _ = await Assert.That(journalDisk.GetProperty("highWaterBytes").GetInt64()).IsEqualTo(journalDiskDetails.HighWaterBytes);
        _ = await Assert.That(journalDisk.GetProperty("writeRejectionActive").GetBoolean()).IsEqualTo(journalDiskDetails.WriteRejectionActive);
        _ = await Assert.That(journalDisk.TryGetProperty("State", out _)).IsFalse();
        _ = await Assert.That(journalDisk.TryGetProperty("MaxBytes", out _)).IsFalse();
        _ = await Assert.That(journalDisk.TryGetProperty("UsedBytes", out _)).IsFalse();
        _ = await Assert.That(journalDisk.TryGetProperty("HighWaterBytes", out _)).IsFalse();
        _ = await Assert.That(journalDisk.TryGetProperty("WriteRejectionActive", out _)).IsFalse();
        _ = await Assert.That(healthElement.TryGetProperty("coordination", out var coordination)).IsTrue();
        _ = await Assert.That(coordination.TryGetProperty("leases", out var leases)).IsTrue();
        _ = await Assert.That(leases.TryGetProperty("pendingGrants", out _)).IsTrue();
        _ = await Assert.That(leases.TryGetProperty("pendingReleases", out _)).IsTrue();
        _ = await Assert.That(leases.TryGetProperty("expired", out _)).IsFalse();
        _ = await Assert.That(leases.TryGetProperty("renewals", out _)).IsFalse();
        _ = await Assert.That(healthElement.TryGetProperty("retentionCleanup", out var retentionCleanup)).IsTrue();
        _ = await Assert.That(retentionCleanup.GetProperty("degraded").GetBoolean()).IsFalse();
        _ = await Assert.That(healthElement.TryGetProperty("JournalBacklogOps", out _)).IsFalse();
    }

    /// <summary>Ensures SerializeToElement can still round-trip application payloads through reflection fallback.</summary>
    [Test]
    public async Task SerializeElementKeepsUnknownTypes()
    {
        var serializer = new ServerJsonSerializer();
        var payload = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["value"] = 42 };

        var element = serializer.SerializeToElement(payload);
        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(element);

        _ = await Assert.That(roundTrip).IsNotNull();
        _ = await Assert.That(roundTrip["value"]).IsEqualTo(42);
    }
}
