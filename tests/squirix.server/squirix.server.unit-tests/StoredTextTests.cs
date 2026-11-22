using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Codecs;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies UTF-8 text storage and serialization.</summary>
public sealed class StoredTextTests
{
    /// <summary>Replacing a borrowed frame cannot change a stored text value.</summary>
    [Test]
    public async Task DirectTextOwnsBorrowedBytes()
    {
        const string input = "a\0Ж漢😀e\u0301";
        var frame = new CacheValue { StringValue = input }.ToByteArray();
        var original = new byte[frame.Length];
        frame.CopyTo(original, 0);
        var reader = new WireReader(frame, frame);
        var decoded = CacheValue.Read(ref reader);
        Array.Fill(frame, byte.MaxValue);
        _ = await Assert.That(decoded.StringValue).IsEqualTo(input);
        _ = await Assert.That(decoded.ToByteArray().AsSpan().SequenceEqual(original)).IsTrue();
        _ = await Assert.That(decoded.EncodedText).IsNotNull();
    }

    /// <summary>The original wire parser and the compact reader both reject invalid UTF-8.</summary>
    [Test]
    public async Task MalformedUtf8IsRejected()
    {
        var reader = new WireReader([2, 2, 0xc0, 0xaf]);
        var rejected = false;
        try
        {
            _ = CacheValue.Read(ref reader);
        }
        catch (InvalidDataException)
        {
            rejected = true;
        }

        _ = await Assert.That(rejected).IsTrue();
    }

    /// <summary>Durable bytes remain identical, including for an empty string.</summary>
    /// <param name="input">Text to round-trip.</param>
    [Test]
    [Arguments("")]
    [Arguments("abcЖ漢😀")]
    public async Task StoredTextUsesOriginalJournalEncoding(string input)
    {
        var expected = new NodeCacheEntry<object?>(input, 13);
        var stored = new NodeCacheEntry<object?>(new StoredText(Encoding.UTF8.GetBytes(input)), 13);
        var original = new byte[CacheEntryCodec.ComputeEncodedLength(expected)];
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(stored)];
        CacheEntryCodec.Write(expected, original);
        CacheEntryCodec.Write(stored, encoded);
        _ = await Assert.That(encoded.AsSpan().SequenceEqual(original)).IsTrue();
        _ = await Assert.That(CacheEntryCodec.TryRead<string>(encoded, out var restored, out var consumed)).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(encoded.Length);
        _ = await Assert.That(restored!.Value).IsEqualTo(input);
    }

    /// <summary>Typed journal reads allocate the result string and entry without an intermediate UTF-8 copy.</summary>
    [Test]
    public async Task TypedJournalReadAvoidsPayloadCopy()
    {
        var input = new string('x', 32 * 1024);
        var entry = new NodeCacheEntry<object?>(input);
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, encoded);
        _ = CacheEntryCodec.TryRead<string>(encoded, out _, out _);

        var before = GC.GetAllocatedBytesForCurrentThread();
        var valid = CacheEntryCodec.TryRead<string>(encoded, out var restored, out var consumed);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        _ = await Assert.That(valid).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(encoded.Length);
        _ = await Assert.That(restored!.Value).IsEqualTo(input);
        _ = await Assert.That(allocated).IsLessThan((Convert.ToInt64(input.Length) * sizeof(char)) + 1024);
    }

    /// <summary>The public local value stays a string for hits and replacements.</summary>
    [Test]
    public async Task ReplacementReturnsLogicalText()
    {
        var cache = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        var key = CacheKey.Default("test");
        cache.Set(key, new NodeCacheEntry<object?>("first"), CancellationToken.None);
        var value = new string('x', 4096);
        cache.Set(key, new NodeCacheEntry<object?>(value), CancellationToken.None);
        var hit = cache.GetValue(key, CancellationToken.None);
        _ = await Assert.That(hit.Found).IsTrue();
        _ = await Assert.That(hit.RawValue is StoredText).IsTrue();
        _ = await Assert.That(hit.Value as string).IsEqualTo(value);
        _ = await Assert.That(cache.GetEntry(key, CancellationToken.None)!.Value as string).IsEqualTo(value);
    }

    /// <summary>Updating with encoded text preserves its identity and exposes the new logical string.</summary>
    [Test]
    public async Task UpdateReusesStoredText()
    {
        var cache = new PhysicalCache<object?>(TimeProvider.System, new ObjectCacheEntrySizeEstimator());
        var key = CacheKey.Default("test");
        cache.Set(key, new NodeCacheEntry<object?>("first"), CancellationToken.None);
        const string replacement = "Ж漢😀";
        var stored = new StoredText(Encoding.UTF8.GetBytes(replacement));

        var updated = cache.Update(key, stored, CancellationToken.None);

        _ = await Assert.That(updated).IsTrue();
        var hit = cache.GetValue(key, CancellationToken.None);
        _ = await Assert.That(hit.Found).IsTrue();
        _ = await Assert.That(hit.RawValue).IsSameReferenceAs(stored);
        _ = await Assert.That(hit.Value as string).IsEqualTo(replacement);
    }
}
