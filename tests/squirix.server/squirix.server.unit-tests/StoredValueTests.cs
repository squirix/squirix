using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.Storage.Codecs;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Checks ownership of slices and the full bit range of inline numbers.</summary>
public sealed class StoredValueTests
{
    /// <summary>A live value occupies at most two 64-bit words per cache node.</summary>
    [Test]
    public async Task StoredValueFitsInSixteenBytes()
    {
        _ = await Assert.That(Unsafe.SizeOf<StoredValue>()).IsLessThanOrEqualTo(16);
        _ = await Assert.That(StoredValue.FromInteger(long.MinValue).Bytes.Length).IsEqualTo(0);
    }

    /// <summary>The durable entry codec restores both the kind and the exact bytes of a typed value.</summary>
    /// <param name="packed">Whether to seal the backing page before exercising the entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DiskRestoresEachValueKind(bool packed, CancellationToken cancellationToken)
    {
        StoredValue[] values =
        [
            default, StoredValue.FromBoolean(false), StoredValue.FromBoolean(true), StoredValue.FromInteger(long.MaxValue),
            StoredValue.FromDouble(-0d), StoredValue.FromText("é"), StoredValue.FromBinary([0, 255]),
            StoredValue.FromJson(System.Text.Encoding.UTF8.GetBytes("""{ "n": 2, "n": 1 }""")),
        ];
        var cache = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        var key = CacheKey.Default("shared");
        if (packed)
            FillPage(cache, key, cancellationToken);
        foreach (var value in values)
        {
            var expected = new NodeCacheEntry<StoredValue>(value, 7);
            cache.Set(key, expected, cancellationToken);
            var entry = cache.GetEntry(key, cancellationToken)!;
            if (value.Kind is StoredValueKind.Text or StoredValueKind.Json or StoredValueKind.Binary)
                _ = await Assert.That(entry.Value.IsSlice).IsEqualTo(packed);
            var bytes = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
            CacheEntryCodec.Write(entry, bytes);
            _ = await Assert.That(CacheEntryCodec.TryRead<StoredValue>(bytes, out var restored, out var consumed)).IsTrue();
            _ = await Assert.That(consumed).IsEqualTo(bytes.Length);
            Array.Clear(bytes);
            _ = await Assert.That(restored!.Version).IsEqualTo(7L);
            _ = await Assert.That(restored.Value.Kind).IsEqualTo(value.Kind);
            var actual = restored.MapToWire().Value!.ToByteArray();
            _ = await Assert.That(actual.AsSpan().SequenceEqual(expected.MapToWire().Value!.ToByteArray())).IsTrue();
        }
    }

    /// <summary>Concurrent replacements keep the scalar bits, payload reference and version of the same write together.</summary>
    /// <param name="packed">Whether to seal the backing page before exercising the entry.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentReplacementsStayCoherent(bool packed, CancellationToken cancellationToken)
    {
        var key = CacheKey.Default("shared");
        var cache = new PhysicalCache<StoredValue>(TimeProvider.System, new CacheEntrySizeEstimator<StoredValue>());
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["kind"] = "text" }.ToFrozenDictionary(StringComparer.Ordinal);
        var text = new NodeCacheEntry<StoredValue>(StoredValue.FromText("one"), 1, DateTime.MaxValue, tags: tags);
        var number = new NodeCacheEntry<StoredValue>(StoredValue.FromInteger(long.MinValue), 2);
        if (packed)
            FillPage(cache, key, cancellationToken);
        var invalid = 0;
        _ = Parallel.For(0, 4, new ParallelOptions { CancellationToken = cancellationToken }, worker =>
        {
            for (var i = 0; i < 4096; i++)
            {
                cache.Set(key, ((i + worker) & 1) == 0 ? text : number, cancellationToken);
                var hit = cache.GetEntry(key, cancellationToken)!;
                var valid = hit.Version == 1
                    ? hit.ExpiresUtc == DateTime.MaxValue && hit.Tags?.ContainsKey("kind") == true && hit.Value.Kind == StoredValueKind.Text && hit.Value.Bytes.AsSpan().SequenceEqual("one"u8)
                    : hit.Version == 2 && hit.ExpiresUtc == null && hit.Tags == null && hit.Value.Kind == StoredValueKind.Integer && hit.Value.Integer == long.MinValue;
                if (!valid)
                    _ = Interlocked.Increment(ref invalid);
            }
        });
        _ = await Assert.That(invalid).IsEqualTo(0);
    }

    /// <summary>Compact deadlines preserve the entire DateTime value and distinguish absence from midnight at tick zero.</summary>
    /// <param name="kind">The kind of the deadline.</param>
    [Test]
    [Arguments(DateTimeKind.Utc)]
    [Arguments(DateTimeKind.Local)]
    [Arguments(DateTimeKind.Unspecified)]
    public async Task DeadlinePreservesTicksAndKind(DateTimeKind kind)
    {
        var absent = new NodeCacheEntry<StoredValue>(default);
        _ = await Assert.That(absent.ExpiresUtc).IsNull();
        foreach (var ticks in new[] { 0L, DateTime.MaxValue.Ticks })
        {
            var expected = new DateTime(ticks, kind);
            var entry = new NodeCacheEntry<StoredValue>(default, expiresUtc: expected);
            _ = await Assert.That(entry.ExpiresUtc!.Value.Ticks).IsEqualTo(ticks);
            _ = await Assert.That(entry.ExpiresUtc.Value.Kind).IsEqualTo(kind);
        }
    }

    /// <summary>A small JSON slice retains only its own bytes when its larger owner is reused.</summary>
    [Test]
    public async Task JsonSliceOwnsOnlyItsPayload()
    {
        var frame = System.Text.Encoding.UTF8.GetBytes("xx[0]yy");
        var value = StoredValue.FromJson(frame.AsMemory(2, 3));
        Array.Clear(frame);
        _ = await Assert.That(value.Kind).IsEqualTo(StoredValueKind.Json);
        _ = await Assert.That(value.Bytes.AsSpan().SequenceEqual("[0]"u8)).IsTrue();
    }

    /// <summary>The sign bit of zero and a NaN payload survive storage without normalization.</summary>
    /// <param name="bits">The IEEE 754 bit pattern.</param>
    [Test]
    [Arguments(long.MinValue)]
    [Arguments(0x7ff8000000000001L)]
    public async Task DoublePreservesBits(long bits)
    {
        var value = StoredValue.FromDouble(BitConverter.Int64BitsToDouble(bits));
        _ = await Assert.That(value.Kind).IsEqualTo(StoredValueKind.Double);
        _ = await Assert.That(BitConverter.DoubleToInt64Bits(value.Double)).IsEqualTo(bits);
    }

    private static void FillPage(PhysicalCache<StoredValue> cache, CacheKey key, CancellationToken cancellationToken)
    {
        var value = new NodeCacheEntry<StoredValue>(StoredValue.FromText("seed"));
        cache.Set(key, value, cancellationToken);
        for (var i = 0; i < 32; i++)
            cache.Set(CacheKey.Default(i.ToString(CultureInfo.InvariantCulture)), value, cancellationToken);
    }
}
