using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using Squirix.Server.Storage.Journaling.Entries;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Unit tests for <see cref="JournalEntryPayload" />.</summary>
public sealed class JournalEntryPayloadTests : UnitTestBase
{
    /// <summary>Put payloads round-trip through the binary cache-entry codec.</summary>
    [Fact]
    public void PutPayloadRoundTripsStringValue()
    {
        var entry = new CacheEntry<string> { Value = "journal-value", Version = 4 };
        var bytes = JournalEntryPayload.Encode(entry);

        Assert.True(JournalEntryPayload.TryDecode<string>(bytes, out var roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal("journal-value", roundTrip.Value);
        Assert.Equal(4, roundTrip.Version);
    }

    /// <summary>Put payloads round-trip entry metadata through the binary cache-entry codec.</summary>
    [Fact]
    public void PutPayloadRoundTripsMetadata()
    {
        var entry = new CacheEntry<string>
        {
            Value = "segmented-value",
            ExpiresUtc = new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc),
            Version = 1_234_567_890_123L,
            Tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["region"] = "west" }.ToFrozenDictionary(StringComparer.Ordinal),
        };
        var bytes = JournalEntryPayload.Encode(entry);

        Assert.True(JournalEntryPayload.TryDecode<string>(bytes, out var roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal(entry.Value, roundTrip.Value);
        Assert.Equal(entry.Version, roundTrip.Version);
        Assert.Equal(entry.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal("west", roundTrip.Tags?["region"]);
    }
}
