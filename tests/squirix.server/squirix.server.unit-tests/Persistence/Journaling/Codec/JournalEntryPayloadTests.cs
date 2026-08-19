using System;
using System.Text.Json;
using JetBrains.Annotations;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Unit tests for <see cref="JournalEntryPayload" />.</summary>
[Immutable]
public sealed class JournalEntryPayloadTests : ServerUnitTestBase
{
    private interface IValueContract;

    /// <summary>
    /// Derived properties on a base/interface-declared entry survive the journal encode/decode round-trip
    /// because <see cref="NodeCacheEntry{T}.Normalize" /> serializes the runtime type.
    /// </summary>
    [Fact]
    public void ObjectEntryRoundTripsRuntimeTypeOfDerivedValue()
    {
        var entry = new NodeCacheEntry<IValueContract>(new DerivedValue { DerivedField = "journal-survives" });
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var buffer = JournalEntryPayload.Encode(in prepared);
        Assert.True(JournalEntryPayload.TryDecode<object?>(buffer.Span, out var roundTrip));
        Assert.NotNull(roundTrip);
        var element = Assert.IsType<JsonElement>(roundTrip.Value);
        Assert.True(element.TryGetProperty("DerivedField", out var field) || element.TryGetProperty("derivedField", out field));
        Assert.Equal("journal-survives", field.GetString());
    }

    /// <summary>Disposing the same pooled payload lease multiple times returns the rented buffer to the pool only once.</summary>
    [Fact]
    public void PooledPayloadDisposeIsIdempotent()
    {
        var prepared = JournalEntryPayload.PrepareEncode(new NodeCacheEntry<string> { Value = "lease", Version = 1 });
        var lease = JournalEntryPayload.Encode(in prepared);
        Assert.Equal(prepared.EncodedLength, lease.Span.Length);
        lease.Dispose();
        lease.Dispose();
        lease.Dispose();
    }

    /// <summary>Put payload round-trip entry metadata through the binary cache-entry codec.</summary>
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

    [Immutable]
    private sealed record DerivedValue : IValueContract
    {
        [UsedImplicitly]
        public string? DerivedField { get; init; }
    }
}
