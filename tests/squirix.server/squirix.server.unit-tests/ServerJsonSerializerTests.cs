using System;
using System.Collections.Generic;
using System.Text.Json;
using Squirix.Server.Adapters.Rest;
using Squirix.Server.Runtime;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Tests for System.Text.Json source-generated metadata used by the default serializer.</summary>
public sealed class ServerJsonSerializerTests : ServerUnitTestBase
{
    /// <summary>Ensures reflection fallback remains available for application payload types.</summary>
    [Fact]
    public void KeepsReflectionFallbackForUnknownApplicationTypes()
    {
        var serializer = new ServerJsonSerializer();
        var payload = serializer.SerializeToUtf8Bytes(new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["value"] = 42 });

        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(payload);

        Assert.NotNull(roundTrip);
        Assert.Equal(42, roundTrip["value"]);
    }

    /// <summary>Ensures health diagnostics DTOs keep stable nested JSON shapes.</summary>
    [Fact]
    public void RestContextPreservesHealthJsonShape()
    {
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
                new HealthJournalDiskDetails("normal", 2048L * 1024 * 1024, 128, 1638L * 1024 * 1024, false)));
        var healthElement = JsonSerializer.SerializeToElement(health, RestJsonSerializerContext.Default.HealthReadyDetailsResponse);

        Assert.True(healthElement.TryGetProperty("journalBacklogOps", out var backlog));
        Assert.Equal(7UL, backlog.GetUInt64());
        Assert.True(healthElement.TryGetProperty("memoryPressure", out var memoryPressure));
        Assert.True(memoryPressure.TryGetProperty("estimatedCacheBytes", out _));
        Assert.True(healthElement.TryGetProperty("journalDisk", out var journalDisk));
        Assert.True(journalDisk.TryGetProperty("usedBytes", out _));
        Assert.True(healthElement.TryGetProperty("retentionCleanup", out var retentionCleanup));
        Assert.False(retentionCleanup.GetProperty("degraded").GetBoolean());
        Assert.False(healthElement.TryGetProperty("JournalBacklogOps", out _));
    }

    /// <summary>Ensures REST response DTOs keep the public web JSON contract.</summary>
    [Fact]
    public void RestContextPreservesPublicResponseJsonShape()
    {
        var element = JsonSerializer.SerializeToElement(new IncrementResponse(42), RestJsonSerializerContext.Default.IncrementResponse);

        Assert.True(element.TryGetProperty("value", out var value));
        Assert.Equal(42, value.GetInt64());
        Assert.False(element.TryGetProperty("Value", out _));

        var error = JsonSerializer.SerializeToElement(new ErrorResponse("missing", "notFound", null), RestJsonSerializerContext.Default.ErrorResponse);

        Assert.True(error.TryGetProperty("error", out _));
        Assert.True(error.TryGetProperty("code", out _));
        Assert.True(error.TryGetProperty("detail", out var detail));
        Assert.Equal(JsonValueKind.Null, detail.ValueKind);
    }

    /// <summary>Ensures SerializeToElement can still round-trip application payloads through reflection fallback.</summary>
    [Fact]
    public void SerializeElementKeepsUnknownApplicationTypes()
    {
        var serializer = new ServerJsonSerializer();
        var payload = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase) { ["value"] = 42 };

        var element = serializer.SerializeToElement(payload);
        var roundTrip = serializer.Deserialize<Dictionary<string, int>>(element);

        Assert.NotNull(roundTrip);
        Assert.Equal(42, roundTrip["value"]);
    }
}
