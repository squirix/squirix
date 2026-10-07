using System;
using System.IO;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>Mutating RPC fingerprints tell apart every pair of different requests.</summary>
[Immutable]
public sealed class RpcMutationFingerprintsTests
{
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
    /// A request parsed from non-canonical wire bytes (fields out of order, a struct split over repeated entry messages) keeps its fingerprint when it is
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

        _ = await Assert.That(set.Entry.Value.Fields.Count).IsEqualTo(2);
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

    /// <summary>Builds the wire bytes of an entry-carrying request whose fields come in reverse order and whose struct arrives as two entry messages.</summary>
    /// <returns>The non-canonical wire bytes, shared by every request type with the same field layout.</returns>
    private static byte[] CreateNonCanonicalWire()
    {
        using var wire = new MemoryStream();
        wire.Write(new SetEntryAsyncRequest { OperationId = "0123456789abcdef0123456789abcdef" }.ToByteArray());
        wire.Write(CreateEntryPiece("z", 1));
        wire.Write(CreateEntryPiece("a", 2));
        wire.Write(new SetEntryAsyncRequest { Key = "key" }.ToByteArray());
        wire.Write(new SetEntryAsyncRequest { CacheName = "cache" }.ToByteArray());
        return wire.ToArray();
    }

    private static byte[] CreateEntryPiece(string field, double number)
    {
        var value = new Struct
        {
            Fields =
            {
                [field] = Value.ForNumber(number),
            },
        };
        return new SetEntryAsyncRequest { Entry = new CacheEntryWire { Value = value } }.ToByteArray();
    }
}
