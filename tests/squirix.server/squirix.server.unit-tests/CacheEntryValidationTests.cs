using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Entry validation preserves decoding rules without materializing values.</summary>
public sealed class CacheEntryValidationTests : ServerUnitTestBase
{
    /// <summary>Every kind accepts its full envelope and rejects every truncated prefix.</summary>
    /// <param name="kind">The value kind.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(4)]
    [Arguments(5)]
    [Arguments(6)]
    [Arguments(7)]
    public async Task KindsAndTruncationsAgree(int kind)
    {
        var bytes = Encoded(kind);
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsTrue();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out _, out var consumed)).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(bytes.Length);
        for (var length = 0; length < bytes.Length; length++)
        {
            _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes.AsSpan(0, length))).IsFalse();
            _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes.AsSpan(0, length), out _, out _)).IsFalse();
        }
    }

    /// <summary>Large values and tags do not allocate while validation runs.</summary>
    /// <param name="kind">The value kind.</param>
    [Test]
    [Arguments(2)]
    [Arguments(3)]
    [Arguments(7)]
    public async Task ValidationDoesNotAllocate(int kind)
    {
        var bytes = Encoded(kind);
        _ = CacheEntryCodec.IsWellFormed(bytes);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var valid = CacheEntryCodec.IsWellFormed(bytes);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        _ = await Assert.That(valid).IsTrue();
        _ = await Assert.That(allocated).IsEqualTo(0L);
    }

    /// <summary>Malformed JSON is rejected even when all outer lengths fit.</summary>
    /// <param name="json">Invalid JSON text.</param>
    [Test]
    [Arguments("")]
    [Arguments("[1,]")]
    [Arguments("{} {}")]
    [Arguments("/*comment*/{}")]
    public async Task InvalidJsonIsRejected(string json)
    {
        var bytes = JsonValueKit.EncodeEntryAround(System.Text.Encoding.UTF8.GetBytes(json));
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out _, out _)).IsFalse();
    }

    /// <summary>The same version exception occurs only after valid contents have been parsed.</summary>
    [Test]
    public async Task VersionRejectionKeepsOrder()
    {
        var bytes = JsonValueKit.EncodeEntryAround("{}"u8);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(2), 0L);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(bytes, static data => _ = CacheEntryCodec.IsWellFormed(data));
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(bytes, static data => _ = CacheEntryCodec.TryRead<object?>(data, out _, out _));
        bytes[^1] = 0;
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out _, out _)).IsFalse();
    }

    /// <summary>Optional fields reject an out-of-range absolute deadline.</summary>
    [Test]
    public async Task InvalidDeadlineIsRejected()
    {
        var bytes = Encoded(0);
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(1), long.MaxValue);
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out _, out _)).IsFalse();
    }

    /// <summary>Unknown kinds and invalid decimal text remain invalid.</summary>
    [Test]
    public async Task InvalidValueKindsAreRejected()
    {
        var bytes = JsonValueKit.EncodeEntryAround("{}"u8);
        bytes[12] = byte.MaxValue;
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsFalse();
        bytes[12] = 6;
        bytes[13] = 1;
        bytes[14] = 0;
        bytes[15] = byte.MaxValue;
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsFalse();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out _, out _)).IsFalse();
    }

    /// <summary>Bytes past a valid entry retain the decoder's prefix acceptance.</summary>
    [Test]
    public async Task TrailingBytesRemainAccepted()
    {
        var bytes = JsonValueKit.EncodeEntryAround("{}"u8);
        byte[] extended = [.. bytes, 255];
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(extended)).IsTrue();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(extended, out _, out var consumed)).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(bytes.Length);
    }

    /// <summary>Duplicate tag names and replacement UTF-8 decoding keep their existing acceptance.</summary>
    [Test]
    public async Task DuplicateTagsRemainAccepted()
    {
        byte[] bytes = [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 1, 0, 255, 1, 0, 65, 1, 0, 255, 1, 0, 66, 0];
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(bytes)).IsTrue();
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(bytes, out var entry, out var consumed)).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(bytes.Length);
        _ = await Assert.That(entry!.Tags!.Count).IsEqualTo(1);
        _ = await Assert.That(entry.Tags["\uFFFD"]).IsEqualTo("B");
    }

    private static byte[] Encoded(int kind)
    {
        object? value = kind switch
        {
            0 => null,
            1 => true,
            2 => new string('x', 4096),
            3 => new byte[4096],
            4 => long.MinValue,
            5 => double.NaN,
            6 => decimal.MaxValue,
            7 => JsonValueKit.Create("[\"" + new string('x', 4096) + "\"]"),
            _ => throw new ArgumentOutOfRangeException(nameof(kind)),
        };
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tag"] = "value" }.ToFrozenDictionary(StringComparer.Ordinal);
        return JsonValueKit.Encode(new NodeCacheEntry<object?>(value, 7, DateTime.UnixEpoch, TimeSpan.FromTicks(-1), tags));
    }
}
