using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.Storage.Journaling;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Checks allocation and byte ownership at the disk boundary.</summary>
public sealed class TypedPersistenceTests
{
    private const int Count = 256;

    /// <summary>Repeatedly sizing and writing a prepared text entry allocates no objects.</summary>
    [Test]
    public async Task EncodingPreparedTextAllocatesNothing()
    {
        var entry = new NodeCacheEntry<StoredValue>(StoredValue.FromText("Привет"));
        var prepared = JournalEntryPayload.PrepareEncode(entry);
        var destination = new byte[prepared.EncodedLength];
        _ = MeasureWrites(prepared, destination);
        var allocated = MeasureWrites(prepared, destination);

        _ = await Assert.That(allocated).IsEqualTo(0L);
        _ = await Assert.That(CacheEntryCodec.TryRead<StoredValue>(destination, out var decoded, out var read)).IsTrue();
        _ = await Assert.That(read).IsEqualTo(destination.Length);
        _ = await Assert.That(decoded!.Value.Bytes.AsSpan().SequenceEqual("Привет"u8)).IsTrue();
    }

    /// <summary>Decoding allocates the same bytes as constructing only the entries and their owned payloads.</summary>
    /// <param name="text">Whether to restore UTF-8 text or an inline integer.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task DecodeAllocatesOnlyRetainedData(bool text)
    {
        var expected = text ? StoredValue.FromText("twenty-three-byte-value") : StoredValue.FromInteger(long.MinValue);
        var entry = new NodeCacheEntry<StoredValue>(expected);
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, encoded);
        var retained = new NodeCacheEntry<StoredValue>?[Count];
        _ = DecodeMany(encoded, retained);
        _ = ConstructMany(text, expected.Bytes.Length, retained);

        var allocated = DecodeMany(encoded, retained);
        var decoded = retained[^1]!.Value;
        var minimum = ConstructMany(text, expected.Bytes.Length, retained);
        Array.Clear(encoded);
        _ = await Assert.That(allocated).IsEqualTo(minimum);
        _ = await Assert.That(decoded.Kind).IsEqualTo(expected.Kind);
        if (text)
            _ = await Assert.That(decoded.Bytes.AsSpan().SequenceEqual(expected.Bytes)).IsTrue();
        else
            _ = await Assert.That(decoded.Integer).IsEqualTo(expected.Integer);
    }

    /// <summary>Preparing a typed entry allocates only the journal holder.</summary>
    [Test]
    public async Task PreparingTypedEntryOnlyAllocatesHolder()
    {
        var typed = new NodeCacheEntry<StoredValue>(StoredValue.FromText("value"));
        var reference = new Tuple<int, NodeCacheEntry<StoredValue>>[Count];
        var retained = new PreparedJournalEntryBase[Count];
        _ = MeasurePreparation(typed, retained);
        _ = MeasureHolders(typed, reference);

        var typedBytes = MeasurePreparation(typed, retained);
        var referenceBytes = MeasureHolders(typed, reference);
        _ = await Assert.That(typedBytes).IsEqualTo(referenceBytes);
        _ = await Assert.That(retained[^1].EncodedLength).IsEqualTo(CacheEntryCodec.ComputeEncodedLength(typed));
    }

    private static long MeasureHolders(NodeCacheEntry<StoredValue> entry, Tuple<int, NodeCacheEntry<StoredValue>>[] retained)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < retained.Length; i++)
            retained[i] = Tuple.Create(i, entry);

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long MeasurePreparation(NodeCacheEntry<StoredValue> entry, PreparedJournalEntryBase[] retained)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < retained.Length; i++)
            retained[i] = JournalEntryPayload.PrepareEncode(entry);

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long ConstructMany(bool text, int length, NodeCacheEntry<StoredValue>?[] retained)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < retained.Length; i++)
            retained[i] = new NodeCacheEntry<StoredValue>(text ? StoredValue.FromText(new byte[length]) : StoredValue.FromInteger(long.MinValue));

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long DecodeMany(byte[] encoded, NodeCacheEntry<StoredValue>?[] retained)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < retained.Length; i++)
        {
            if (!CacheEntryCodec.TryRead<StoredValue>(encoded, out retained[i], out _))
                throw new InvalidOperationException("The test entry must decode.");
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long MeasureWrites(PreparedJournalEntryBase prepared, byte[] destination)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Count; i++)
        {
            if (prepared.Measure() != destination.Length)
                throw new InvalidOperationException("Unexpected encoded length.");
            JournalEntryPayload.WritePrepared(in prepared, destination);
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }
}
