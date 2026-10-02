using System;
using System.Threading.Tasks;
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
}
