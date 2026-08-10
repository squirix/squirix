using System;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Unit tests for <see cref="JournalEntryPayload" />.</summary>
public sealed class JournalEntryPayloadTests : ServerUnitTestBase
{
    /// <summary>Put payloads round-trip entry metadata through the binary cache-entry codec.</summary>
    [Fact]
    public void PutPayloadRoundTripsMetadata()
    {
        var entry = new NodeCacheEntry<string>("segmented-value", 1_234_567_890_123L, new DateTime(2026, 6, 1, 12, 0, 0, DateTimeKind.Utc), tags: EntryTagsKit.RegionWest);
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var buffer = JournalEntryPayload.Encode(in prepared);
        Assert.True(JournalEntryPayload.TryDecode<string>(buffer.Span, out var roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal(entry.Value, roundTrip.Value);
        Assert.Equal(entry.Version, roundTrip.Version);
        Assert.Equal(entry.ExpiresUtc, roundTrip.ExpiresUtc);
        Assert.Equal("west", roundTrip.Tags?["region"]);
    }

    /// <summary>Put payloads round-trip through the binary cache-entry codec.</summary>
    [Fact]
    public void PutPayloadRoundTripsStringValue()
    {
        var entry = new NodeCacheEntry<string> { Value = "journal-value", Version = 4 };
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var buffer = JournalEntryPayload.Encode(in prepared);
        Assert.True(JournalEntryPayload.TryDecode<string>(buffer.Span, out var roundTrip));
        Assert.NotNull(roundTrip);
        Assert.Equal("journal-value", roundTrip.Value);
        Assert.Equal(4, roundTrip.Version);
    }
}
