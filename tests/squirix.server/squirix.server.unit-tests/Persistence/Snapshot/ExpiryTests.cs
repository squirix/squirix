using System;
using System.Buffers.Binary;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Snapshot;

/// <summary>Expiry filtering validates stored entries without materializing values that will be discarded.</summary>
[Immutable]
public sealed class ExpiryTests : IsolatedStorageTestBase
{
    /// <summary>The exact cutoff is excluded; future, unlimited and relative-only entries remain unchanged.</summary>
    /// <param name="windowSize">The reader window size.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(16)]
    [Arguments(65_536)]
    public async Task CutoffKeepsBoundaryAndLiveEntries(int windowSize, CancellationToken cancellationToken)
    {
        var now = LoadSupport.FixedUtc;
        byte[] value = [1, 2, 3];
        var path = await new SnapshotWriter(Dir).WriteAsync(
            1,
            [
                (CacheKey.Default("expired"), ExpiredEntry(0)),
                (CacheKey.Default("boundary"), new NodeCacheEntry<object?>("old", 1, now)),
                (CacheKey.Default("live"), new NodeCacheEntry<object?>(value, 2, now.AddMilliseconds(1))),
                (CacheKey.Default("unlimited"), new NodeCacheEntry<object?>(7, 3)),
                (CacheKey.Default("relative"), new NodeCacheEntry<object?>(8, 4, expiration: TimeSpan.FromMinutes(1))),
            ],
            [],
            cancellationToken);
        var reader = StoreFactory.CreateReader(windowSize);

        var all = await reader.LoadStrictAsync<object?>(path, null, cancellationToken);
        var loaded = await reader.LoadStrictAsync<object?>(path, now, cancellationToken);

        _ = await Assert.That((all.Entries.Count, loaded.Entries.Count, loaded.EncodedLengths.Count)).IsEqualTo((5, 3, 3));
        _ = await Assert.That((loaded.Entries[0].Key.Key, loaded.Entries[1].Key.Key, loaded.Entries[2].Key.Key)).IsEqualTo(("live", "unlimited", "relative"));
        _ = await Assert.That(loaded.Entries[0].Entry.Value is byte[] bytes && bytes.AsSpan().SequenceEqual(value)).IsTrue();
        _ = await Assert.That((loaded.Entries[0].Entry.Version, loaded.Entries[0].Entry.ExpiresUtc)).IsEqualTo((2L, now.AddMilliseconds(1)));
        _ = await Assert.That(loaded.Entries[2].Entry.Expiration).IsEqualTo(TimeSpan.FromMinutes(1));
    }

    /// <summary>Expired malformed values still fail like the materializing reader, even with valid outer checksums.</summary>
    /// <param name="damage">Invalid JSON, a truncated value or an unknown value kind.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task ExpiredCorruptionMatchesOracle(int damage, CancellationToken cancellationToken)
    {
        var body = EncodeExpiredBody(out var entryStart);
        if (damage == 0)
            body[^1] = 120;
        else if (damage == 1)
            body = body[..^1];
        else
            body[entryStart + sizeof(byte) + sizeof(long) + sizeof(byte) + sizeof(long) + sizeof(ushort)] = 255;
        var path = await WriteBodyAsync(body, cancellationToken);

        var (_, _, expected) = await LoadSupport.LoadOutcomeAsync(new LegacyReader(), path, LoadSupport.FixedUtc, cancellationToken);
        var (_, _, actual) = await LoadSupport.LoadOutcomeAsync(StoreFactory.CreateReader(16), path, LoadSupport.FixedUtc, cancellationToken);

        _ = await Assert.That(expected).IsNotNull();
        _ = await Assert.That(actual).IsEqualTo(expected);
    }

    /// <summary>Expiry does not hide the entry constructor's rejection of version zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredInvalidVersionMatchesOracle(CancellationToken cancellationToken)
    {
        var body = EncodeExpiredBody(out var entryStart);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(entryStart + sizeof(byte) + sizeof(long) + sizeof(byte)), 0);
        var path = await WriteBodyAsync(body, cancellationToken);

        var expected = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((path, cancellationToken), static s => _ = new LegacyReader().LoadStrictAsync<object?>(s.path, LoadSupport.FixedUtc, s.cancellationToken).AsTask());
        var actual = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((path, cancellationToken), static s => _ = StoreFactory.CreateReader(16).LoadStrictAsync<object?>(s.path, LoadSupport.FixedUtc, s.cancellationToken).AsTask());

        _ = await Assert.That(actual.Message).IsEqualTo(expected.Message);
    }

    /// <summary>An expired value of another type is skipped before mapping into the requested cache type.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredTypeMismatchDoesNotBlockLoad(CancellationToken cancellationToken)
    {
        var path = await new SnapshotWriter(Dir).WriteAsync(
            1,
            [
                (CacheKey.Default("expired"), ExpiredEntry(0)),
                (CacheKey.Default("live"), new NodeCacheEntry<object?>(7)),
            ],
            [],
            cancellationToken);

        var loaded = await StoreFactory.CreateReader(16).LoadStrictAsync<int>(path, LoadSupport.FixedUtc, cancellationToken);

        _ = await Assert.That(loaded.Entries.Count).IsEqualTo(1);
        _ = await Assert.That((loaded.Entries[0].Key.Key, loaded.Entries[0].Entry.Value)).IsEqualTo(("live", 7));
    }

    /// <summary>Expired JSON, string and binary values validate successfully without creating an entry.</summary>
    /// <param name="kind">The value shape.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task ExpiredValuesAreNotMaterialized(int kind)
    {
        var entry = ExpiredEntry(kind);
        var bytes = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, bytes);

        var valid = CacheEntryCodec.TryRead<object?>(bytes, out var skipped, out var consumed, LoadSupport.FixedUtc);
        var retained = CacheEntryCodec.TryRead<object?>(bytes, out var loaded, out var loadedLength);

        _ = await Assert.That((valid, consumed, retained, loadedLength)).IsEqualTo((true, bytes.Length, true, bytes.Length));
        _ = await Assert.That(skipped).IsNull();
        _ = await Assert.That(loaded).IsNotNull();
        _ = await Assert.That(loaded!.Value).IsNotNull();
        _ = await Assert.That(loaded.Tags!["tag"]).IsEqualTo("retained");
    }

    private static byte[] EncodeExpiredBody(out int entryStart)
    {
        var entry = new NodeCacheEntry<object?>(JsonUtf8Value.FromSerialized(Encoding.UTF8.GetBytes("{\"a\":1}")), 1, LoadSupport.FixedUtc.AddMinutes(-1));
        var key = CacheKey.Default("expired");
        var body = new byte[SnapshotCodec.ComputeEntryBodyLength(key, entry)];
        SnapshotCodec.WriteEntryBody(key, entry, body);
        entryStart = sizeof(ushort) + Encoding.UTF8.GetByteCount(key.Namespace) + sizeof(ushort) + Encoding.UTF8.GetByteCount(key.Key);
        return body;
    }

    private static NodeCacheEntry<object?> ExpiredEntry(int kind)
    {
        object value = kind switch
        {
            0 => JsonUtf8Value.FromSerialized(Encoding.UTF8.GetBytes("\"" + new string('x', 4096) + "\"")),
            1 => new string('x', 4096),
            _ => new byte[4096],
        };
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tag"] = "retained" }.ToFrozenDictionary(StringComparer.Ordinal);
        return new NodeCacheEntry<object?>(value, 1, LoadSupport.FixedUtc.AddMinutes(-1), tags: tags);
    }

    private async Task<string> WriteBodyAsync(byte[] body, CancellationToken cancellationToken)
    {
        var record = new byte[SnapshotCodec.ComputeRecordLength(body.Length)];
        body.CopyTo(record, SnapshotCodec.RecordHeaderSize);
        SnapshotCodec.SealRecord(record, RecordKind.Entry);
        var path = Path.Join(Dir, "expired.snap");
        await File.WriteAllBytesAsync(path, LoadSupport.AssembleFile(record), cancellationToken);
        return path;
    }
}
