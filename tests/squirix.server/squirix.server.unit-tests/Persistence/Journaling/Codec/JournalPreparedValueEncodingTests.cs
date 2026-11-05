using System;
using System.Buffers;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
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

    /// <summary>Preparing immutable JSON does not read or copy its payload; encoding reads it once per final entry.</summary>
    [Test]
    public async Task JsonPayloadIsReadOnlyDuringFinalEncoding()
    {
        var text = "\"" + new string('x', 65536) + "\"";
        var bytes = Encoding.UTF8.GetBytes(text);
        using var memory = new ReadCountingMemory(bytes);
        _ = await Assert.That(JsonUtf8Value.TryCreate(memory.Bytes, out var json)).IsTrue();
        var readsBefore = memory.SpanReads;
        using var value = JournalEntryPayload.PrepareValue(json);
        _ = await Assert.That(memory.SpanReads - readsBefore).IsEqualTo(0);
        _ = await Assert.That(value.EncodedLength).IsEqualTo(bytes.Length + 5);

        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["team"] = "json" }.ToFrozenDictionary(StringComparer.Ordinal);
        var entry = new NodeCacheEntry<object?>(json, 7, Deadline, tags: tags);
        using var encoded = JournalEntryPayload.EncodeWithPreparedValue(entry, value);
        _ = await Assert.That(memory.SpanReads - readsBefore).IsEqualTo(1);
        using var repeated = JournalEntryPayload.EncodeWithPreparedValue(entry, value);
        _ = await Assert.That(memory.SpanReads - readsBefore).IsEqualTo(2);
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        using var whole = JournalEntryPayload.Encode(in prepared);
        _ = await Assert.That(SameBytes(whole, encoded)).IsTrue();
        _ = await Assert.That(SameBytes(whole, repeated)).IsTrue();
        value.Dispose();
        value.Dispose();
        _ = await Assert.That(Encoding.UTF8.GetString(bytes)).IsEqualTo(text);
    }

    /// <summary>Mutable binary input is snapshotted during preparation before the caller can change it.</summary>
    [Test]
    public async Task BinaryPreparationRetainsOriginalBytes()
    {
        byte[] bytes = [1, 2, 3, 4];
        using var value = JournalEntryPayload.PrepareValue(bytes);
        Array.Fill<byte>(bytes, 9);
        var entry = new NodeCacheEntry<object?>(bytes, 7, Deadline);
        using var encoded = JournalEntryPayload.EncodeWithPreparedValue(entry, value);
        var expected = JournalEntryPayload.PrepareEncode(new NodeCacheEntry<object?>(new byte[] { 1, 2, 3, 4 }, 7, Deadline));
        using var whole = JournalEntryPayload.Encode(in expected);

        _ = await Assert.That(SameBytes(whole, encoded)).IsTrue();
    }

    /// <summary>An oversized JSON value is rejected before preparation reads its payload.</summary>
    [Test]
    public async Task OversizedJsonIsRejectedBeforeCopy()
    {
        using var memory = new ReadCountingMemory(JsonBytes(EntryLimits.MaxEntrySizeBytes + 1));
        _ = await Assert.That(JsonUtf8Value.TryCreate(memory.Bytes, out var json)).IsTrue();
        var readsBefore = memory.SpanReads;
        var error = NodeExceptionAssert.For<SquirixException>().Throws(
            json,
            static value =>
            {
                using var prepared = JournalEntryPayload.PrepareValue(value);
            });

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        _ = await Assert.That(memory.SpanReads).IsEqualTo(readsBefore);
    }

    /// <summary>A JSON value at the value limit is prepared, but its oversized entry is refused before reading the payload.</summary>
    [Test]
    public async Task JsonAtLimitRejectsEntryOverhead()
    {
        using var memory = new ReadCountingMemory(JsonBytes(EntryLimits.MaxEntrySizeBytes));
        _ = await Assert.That(JsonUtf8Value.TryCreate(memory.Bytes, out var json)).IsTrue();
        var readsBefore = memory.SpanReads;
        using var value = JournalEntryPayload.PrepareValue(json);
        _ = await Assert.That(value.EncodedLength).IsEqualTo(EntryLimits.MaxEntrySizeBytes);
        var error = NodeExceptionAssert.For<SquirixException>().Throws(
            value,
            static prepared =>
            {
                using var encoded = JournalEntryPayload.EncodeWithPreparedValue(new NodeCacheEntry<object?>(null, 7, Deadline), prepared);
            });

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.PayloadTooLarge);
        _ = await Assert.That(memory.SpanReads).IsEqualTo(readsBefore);
    }

    private static byte[] JsonBytes(int encodedLength)
    {
        var bytes = new byte[encodedLength - 5];
        Array.Fill<byte>(bytes, 120);
        bytes[0] = 34;
        bytes[^1] = 34;
        return bytes;
    }

    private static bool SameBytes(PooledJournalPayload left, PooledJournalPayload right) => left.Memory.Span.SequenceEqual(right.Memory.Span);

    [Mutable]
    private sealed class ReadCountingMemory : MemoryManager<byte>
    {
        private readonly byte[] _bytes;

        internal ReadCountingMemory(byte[] bytes)
        {
            _bytes = bytes;
        }

        internal int SpanReads { get; private set; }

        internal ReadOnlyMemory<byte> Bytes => CreateMemory(_bytes.Length);

        public override Span<byte> GetSpan()
        {
            SpanReads++;
            return _bytes;
        }

        public override MemoryHandle Pin(int elementIndex = 0) => throw new NotSupportedException();

        public override void Unpin()
        {
        }

        protected override void Dispose(bool disposing)
        {
        }
    }
}
