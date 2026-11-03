using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Replica entry encoding preserves journal bytes while returning independently owned storage.</summary>
public sealed class ReplicaEntryEncodingTests : ServerUnitTestBase
{
    /// <summary>Direct encoding is byte-compatible with the journal codec for each supported value shape.</summary>
    /// <param name="kind">The value shape.</param>
    /// <param name="metadata">Whether the entry has a deadline and tags.</param>
    [Test]
    [Arguments(0, false)]
    [Arguments(1, false)]
    [Arguments(2, false)]
    [Arguments(3, false)]
    [Arguments(4, false)]
    [Arguments(5, false)]
    [Arguments(6, false)]
    [Arguments(7, false)]
    [Arguments(8, false)]
    [Arguments(9, false)]
    [Arguments(7, true)]
    public async Task EncodingMatchesJournalBytes(int kind, bool metadata)
    {
        object? value = kind switch
        {
            0 => null,
            1 => true,
            2 => string.Empty,
            3 => new string('\u00E9', 4096),
            4 => new byte[] { 1, 2, 3, 4 },
            5 => long.MinValue,
            6 => 1.25d,
            7 => JsonValueKit.Create("""{"n":12345678901234567890,"text":"hello"}"""),
            8 => 123.456m,
            _ => JsonElement.Parse("""[1,true,{"n":2}]"""),
        };
        var tags = metadata ? new Dictionary<string, string>(StringComparer.Ordinal) { ["tag"] = "value" }.ToFrozenDictionary(StringComparer.Ordinal) : null;
        DateTime? deadline = metadata ? new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc) : null;
        var entry = new NodeCacheEntry<object?>(value, 7, deadline, null, tags);
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var expected = JournalEntryPayload.Encode(in prepared);

        var actual = ReplicaCacheApplier.EncodeEntry(entry);
        var owned = JournalEntryPayload.EncodeOwned(in prepared);

        await SequenceAssert.EqualAsync(expected.Memory.ToArray(), actual);
        await SequenceAssert.EqualAsync(actual, owned);
        _ = await Assert.That(JournalEntryPayload.TryDecode<object?>(actual, out var decoded)).IsTrue();
        _ = await Assert.That(decoded!.Version).IsEqualTo(7L);
        _ = await Assert.That(decoded.ExpiresUtc).IsEqualTo(deadline);
        if (metadata)
            _ = await Assert.That(decoded.Tags!["tag"]).IsEqualTo("value");
    }

    /// <summary>The journal size limit includes the deadline even though the replica payload stores it separately.</summary>
    [Test]
    public async Task ReplicaUpsertIncludesDeadlineAtSizeLimit()
    {
        var overhead = JournalEntryPayload.MeasureSerializedBytes(new NodeCacheEntry<object?>(Array.Empty<byte>()));
        var value = new byte[EntryLimits.MaxEntrySizeBytes - overhead];
        var prepared = PreparedJournalEntry.ForReplicaUpsert(value, 1, null, null);
        _ = await Assert.That(prepared.EncodedLength).IsEqualTo(EntryLimits.MaxEntrySizeBytes);

        var error = NodeExceptionAssert.For<SquirixException>().Throws(value, static bytes =>
            _ = PreparedJournalEntry.ForReplicaUpsert(bytes, 1, null, DateTime.UnixEpoch));
        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
    }

    /// <summary>A destination longer than the measured entry cannot expose an unwritten suffix.</summary>
    [Test]
    public async Task OwnedEncodingRejectsLengthMismatch()
    {
        var entry = new NodeCacheEntry<object?>("value");
        var measured = JournalEntryPayload.PrepareEncode(entry);
        var wrong = JournalEntryPayload.PrepareEncode(entry, measured.EncodedLength + 1);

        var error = NodeExceptionAssert.For<InvalidOperationException>().Throws(wrong, static candidate => _ = JournalEntryPayload.EncodeOwned(in candidate));

        _ = await Assert.That(error.Message).Contains("measured", StringComparison.Ordinal);
    }

    /// <summary>Encoding does not expose the source bytes or share a mutable destination across calls.</summary>
    [Test]
    public async Task EncodingOwnsItsOutput()
    {
        byte[] value = [1, 2, 3, 4];
        var entry = new NodeCacheEntry<object?>(value);
        var first = ReplicaCacheApplier.EncodeEntry(entry);
        var second = ReplicaCacheApplier.EncodeEntry(entry);

        Array.Clear(value);
        Array.Clear(second);

        _ = await Assert.That(JournalEntryPayload.TryDecode<object?>(first, out var decoded)).IsTrue();
        var stored = (await Assert.That(decoded!.Value).IsTypeOf<byte[]>())!;
        await SequenceAssert.EqualAsync(new byte[] { 1, 2, 3, 4 }, stored);
    }
}
