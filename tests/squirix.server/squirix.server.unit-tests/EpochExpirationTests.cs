using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies explicit epoch deadlines survive wire decoding.</summary>
[Immutable]
public sealed class EpochExpirationTests : ServerUnitTestBase
{
    /// <summary>An absent timestamp remains absent after protobuf serialization.</summary>
    [Test]
    public async Task MissingDeadlineRemainsAbsentTest()
    {
        var entry = new NodeCacheEntry<string>("value", expiration: TimeSpan.FromSeconds(9));
        var wire = CacheEntryWire.Parser.ParseFrom(Google.Protobuf.MessageExtensions.ToByteArray(entry.MapToProto()));
        var decoded = wire.MapFromProto<string>();

        _ = await Assert.That(wire.ExpiresUtc).IsNull();
        _ = await Assert.That(decoded.ExpiresUtc).IsNull();
        _ = await Assert.That(decoded.Expiration).IsEqualTo(TimeSpan.FromSeconds(9));
    }

    /// <summary>Compares local expiration with the same entry after protobuf round trip.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EpochMappingKeepsEntryExpiredTest(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        var cache = new PhysicalCache<string>(clock, new CacheEntrySizeEstimator<string>());
        var entry = new NodeCacheEntry<string>("expired", expiresUtc: DateTime.UnixEpoch);
        cache.Set(CacheKey.Default("direct"), entry, cancellationToken);
        var wire = CacheEntryWire.Parser.ParseFrom(Google.Protobuf.MessageExtensions.ToByteArray(entry.MapToProto()));
        var decoded = wire.MapFromProto<string>();
        cache.Set(CacheKey.Default("wire"), decoded, cancellationToken);

        _ = await Assert.That(wire.ExpiresUtc).IsEqualTo(Timestamp.FromDateTime(DateTime.UnixEpoch));
        _ = await Assert.That(cache.GetValue(CacheKey.Default("direct"), cancellationToken).Found).IsFalse();
        _ = await Assert.That(decoded.ExpiresUtc).IsEqualTo(DateTime.UnixEpoch);
        clock.Advance(TimeSpan.FromDays(365));
        _ = await Assert.That(cache.GetValue(CacheKey.Default("wire"), cancellationToken).Found).IsFalse();
    }
}
