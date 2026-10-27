using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>An entry assembled around a prepared value encodes to the same bytes as the same entry encoded whole.</summary>
[Immutable]
public sealed class JournalPreparedValueEncodingTests : ServerUnitTestBase
{
    private static readonly DateTime Deadline = new(2030, 1, 2, 3, 4, 5, 6, DateTimeKind.Utc);

    /// <summary>Every value kind, with tags and an absolute deadline, encodes identically through both paths.</summary>
    [Test]
    public async Task PreparedValueMatchesWholeEntryEncoding()
    {
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "a", ["zone"] = "é中" }.ToFrozenDictionary(StringComparer.Ordinal);
        using var objectDocument = JsonDocument.Parse("{\"a\":1,\"b\":[true,null,\"x\"]}");
        using var arrayDocument = JsonDocument.Parse("[1,2.5,\"y\",{\"k\":false}]");
        object?[] values =
        [
            null,
            true,
            "plain",
            "héllo ✓ \U0001F600",
            new byte[] { 0, 1, 2, 255 },
            42,
            long.MaxValue,
            3.25d,
            12.345m,
            objectDocument.RootElement.Clone(),
            arrayDocument.RootElement.Clone(),
            new SamplePayload { Id = 7, Name = "n", Tags = ["t1", "t2"] },
        ];

        foreach (var value in values)
        {
            var entry = new NodeCacheEntry<object?>(value, 5, Deadline, tags: tags);
            var prepared = JournalEntryPayload.PrepareEncode(entry);
            using var whole = JournalEntryPayload.Encode(in prepared);
            using var preparedValue = JournalEntryPayload.PrepareValue(value);
            using var spliced = JournalEntryPayload.EncodeWithPreparedValue(entry, preparedValue);

            _ = await Assert.That(SameBytes(whole, spliced)).IsTrue();
        }
    }

    /// <summary>An entry with a relative expiration cannot be assembled around a prepared value.</summary>
    [Test]
    public void RelativeExpirationIsRejected()
    {
        var entry = new NodeCacheEntry<object?>("v", expiration: TimeSpan.FromMinutes(1));

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            entry,
            static candidate =>
            {
                using var preparedValue = JournalEntryPayload.PrepareValue<object?>("v");
                using var payload = JournalEntryPayload.EncodeWithPreparedValue(candidate, preparedValue);
            });
    }

    private static bool SameBytes(PooledJournalPayload left, PooledJournalPayload right) => left.Memory.Span.SequenceEqual(right.Memory.Span);
}
