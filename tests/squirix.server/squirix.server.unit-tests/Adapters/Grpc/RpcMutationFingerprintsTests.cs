using System;
using System.Buffers.Binary;
using System.IO;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>Mutating RPC fingerprints tell apart every pair of different requests.</summary>
[Immutable]
public sealed class RpcMutationFingerprintsTests
{
    /// <summary>The chunked message hash gives the fingerprint of the one-shot hash over the whole serialized message at every size around the chunk boundary.</summary>
    /// <param name="messageBytes">The serialized message size; zero means an empty entry.</param>
    [Test]
    [Arguments(0)]
    [Arguments(100)]
    [Arguments(512)]
    [Arguments(513)]
    [Arguments(IncrementalHashBufferWriter.ChunkBytes - 1)]
    [Arguments(IncrementalHashBufferWriter.ChunkBytes)]
    [Arguments(IncrementalHashBufferWriter.ChunkBytes + 1)]
    [Arguments(2 * IncrementalHashBufferWriter.ChunkBytes)]
    [Arguments(4 * 1024 * 1024)]
    public async Task ChunkedHashMatchesOneShotHash(int messageBytes)
    {
        var entry = CreateEntryOfSize(messageBytes);

        _ = await Assert.That(entry.CalculateSize()).IsEqualTo(messageBytes);
        _ = await Assert.That(RpcMutationFingerprints.SetEntry("cache", "key", entry)).IsEqualTo(ComputeOneShotFingerprint("set-entry-async", "cache", "key", entry));
        _ = await Assert.That(RpcMutationFingerprints.Update("cache", "key", entry)).IsEqualTo(ComputeOneShotFingerprint("update-async", "cache", "key", entry));
    }

    /// <summary>Requests whose fields only differ in where the cache name ends and the key begins have different fingerprints.</summary>
    [Test]
    public async Task FieldBoundariesAreUnambiguous()
    {
        var remove = RpcMutationFingerprints.Remove("c", "remove-expiration-asyncK");
        var removeExpiration = RpcMutationFingerprints.RemoveExpiration("cremove-async", "K");
        var shiftedKey = RpcMutationFingerprints.Remove("ca", "b");
        var shiftedName = RpcMutationFingerprints.Remove("c", "ab");

        _ = await Assert.That(remove).IsNotEqualTo(removeExpiration);
        _ = await Assert.That(shiftedKey).IsNotEqualTo(shiftedName);
    }

    /// <summary>
    /// A request parsed from non-canonical wire bytes (fields out of order, an entry split over repeated entry messages) keeps its fingerprint when it is
    /// serialized and parsed again, as the owner does with a forwarded request.
    /// </summary>
    [Test]
    public async Task FingerprintSurvivesReserialization()
    {
        var wire = CreateNonCanonicalWire();
        var set = SetEntryAsyncRequest.Parser.ParseFrom(wire);
        var add = TryAddEntryAsyncRequest.Parser.ParseFrom(wire);
        var update = UpdateAsyncRequest.Parser.ParseFrom(wire);
        var getOrAdd = GetOrAddAsyncRequest.Parser.ParseFrom(wire);
        var setAgain = SetEntryAsyncRequest.Parser.ParseFrom(set.ToByteArray());
        var addAgain = TryAddEntryAsyncRequest.Parser.ParseFrom(add.ToByteArray());
        var updateAgain = UpdateAsyncRequest.Parser.ParseFrom(update.ToByteArray());
        var getOrAddAgain = GetOrAddAsyncRequest.Parser.ParseFrom(getOrAdd.ToByteArray());

        _ = await Assert.That(set.Entry.Value.JsonUtf8.Length).IsGreaterThan(0);
        _ = await Assert.That(set.Entry.Expiration).IsNotNull();
        _ = await Assert.That(RpcMutationFingerprints.SetEntry(set.CacheName, set.Key, set.Entry)).IsEqualTo(RpcMutationFingerprints.SetEntry(setAgain.CacheName, setAgain.Key, setAgain.Entry));
        _ = await Assert.That(RpcMutationFingerprints.AddEntryIfAbsent(add.CacheName, add.Key, add.Entry))
           .IsEqualTo(RpcMutationFingerprints.AddEntryIfAbsent(addAgain.CacheName, addAgain.Key, addAgain.Entry));
        _ = await Assert.That(RpcMutationFingerprints.Update(update.CacheName, update.Key, update.Entry))
           .IsEqualTo(RpcMutationFingerprints.Update(updateAgain.CacheName, updateAgain.Key, updateAgain.Entry));
        _ = await Assert.That(RpcMutationFingerprints.GetOrAdd(getOrAdd.CacheName, getOrAdd.Key, getOrAdd.Entry))
           .IsEqualTo(RpcMutationFingerprints.GetOrAdd(getOrAddAgain.CacheName, getOrAddAgain.Key, getOrAddAgain.Entry));
    }

    /// <summary>The same request always has the same fingerprint, and another entry or expiration changes it.</summary>
    [Test]
    public async Task SameRequestSameFingerprint()
    {
        var entry = new CacheEntryWire { Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)) };
        var other = new CacheEntryWire { Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(2)) };

        _ = await Assert.That(RpcMutationFingerprints.SetEntry("c", "k", entry)).IsEqualTo(RpcMutationFingerprints.SetEntry("c", "k", entry.Clone()));
        _ = await Assert.That(RpcMutationFingerprints.SetEntry("c", "k", other)).IsNotEqualTo(RpcMutationFingerprints.SetEntry("c", "k", entry));
        _ = await Assert.That(RpcMutationFingerprints.Touch("c", "k", Duration.FromTimeSpan(TimeSpan.FromMinutes(5))))
           .IsNotEqualTo(RpcMutationFingerprints.Touch("c", "k", Duration.FromTimeSpan(TimeSpan.FromHours(1))));
        _ = await Assert.That(RpcMutationFingerprints.SetEntry("c", "k", entry)).IsNotEqualTo(RpcMutationFingerprints.Update("c", "k", entry));
    }

    private static string ComputeOneShotFingerprint(string operation, string cacheName, string key, IMessage message)
    {
        using var buffer = new MemoryStream();
        WriteField(buffer, operation);
        WriteField(buffer, cacheName);
        WriteField(buffer, key);
        buffer.Write(SHA256.HashData(message.ToByteArray()));
        return Convert.ToHexString(SHA256.HashData(buffer.ToArray()));
    }

    private static CacheEntryWire CreateEntryOfSize(int messageBytes)
    {
        if (messageBytes == 0)
            return new CacheEntryWire();

        const int maxOverheadBytes = 32;
        for (var length = Math.Max(0, messageBytes - maxOverheadBytes); length <= messageBytes; length++)
        {
            var entry = new CacheEntryWire { Value = new CacheValue { StringValue = new string('a', length) } };
            if (entry.CalculateSize() == messageBytes)
                return entry;
        }

        throw new InvalidOperationException("No entry has the requested serialized size.");
    }

    private static byte[] CreateEntryPiece(CacheEntryWire piece) => new SetEntryAsyncRequest { Entry = piece }.ToByteArray();

    /// <summary>Builds the wire bytes of an entry-carrying request whose fields come in reverse order and whose entry arrives as two entry messages.</summary>
    /// <returns>The non-canonical wire bytes, shared by every request type with the same field layout.</returns>
    private static byte[] CreateNonCanonicalWire()
    {
        using var wire = new MemoryStream();
        wire.Write(new SetEntryAsyncRequest { OperationId = "0123456789abcdef0123456789abcdef" }.ToByteArray());
        wire.Write(CreateEntryPiece(new CacheEntryWire { Value = JsonValueKit.Wire("{\"z\":1}") }));
        wire.Write(CreateEntryPiece(new CacheEntryWire { Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(1)) }));
        wire.Write(new SetEntryAsyncRequest { Key = "key" }.ToByteArray());
        wire.Write(new SetEntryAsyncRequest { CacheName = "cache" }.ToByteArray());
        return wire.ToArray();
    }

    private static void WriteField(MemoryStream destination, string value)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, value.Length);
        destination.Write(length);
        destination.Write(Encoding.Unicode.GetBytes(value));
    }
}
