using System;
using System.Collections.Generic;
using System.Text.Json;
using Squirix.Server.Serialization;
using Squirix.Server.UnitTests.Support;
using Xunit;
using static Squirix.Server.Adapters.Rest.RestDtos;
using RestJsonSerializerContext = Squirix.Server.Adapters.Endpoint.Rest.RestJsonSerializerContext;

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
}
