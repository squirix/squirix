using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An entry decoded from the journal, a snapshot or the replica log is measurable for every payload kind the codec writes, so the
/// follower apply and the recovery insert, which size the decoded entry, cannot fail on the estimator.
/// </summary>
[Immutable]
public sealed class ObjectEstimatorDecodedEntryTests
{
    /// <summary>Every payload kind the entry codec writes decodes into an entry the estimator measures, and the cache records that measure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryDecodedPayloadKindIsMeasurable(CancellationToken cancellationToken)
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var cache = new PhysicalCache<object?>(TimeProvider.System, estimator);
        ILocalCacheStats stats = cache;
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "t1" }.ToFrozenDictionary();
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var tree = JsonElement.Parse("""{"a":[1,"x",true,false,null,{"b":2.5}],"c":"d","e":{}}""");
        object?[] values = [null, true, "text", new byte[] { 1, 2, 3 }, 42, 42L, 1.5d, 12.5m, tree, default(JsonElement), new AdmissionDataPayload { Data = "payload" }];
        long expected = 0;

        for (var i = 0; i < values.Length; i++)
        {
            var key = new CacheKey("ns", $"k{i}");
            var bytes = JournalEntryPayloadKit.Encode(new NodeCacheEntry<object?>(values[i], 3, deadline, null, tags));

            var isDecoded = JournalEntryPayload.TryDecode<object?>(bytes, out var decoded);
            _ = await Assert.That(isDecoded).IsTrue();
            await cache.InsertRecoveryAsync(key, decoded!, cancellationToken);

            var measured = estimator.EstimateBytes(key, decoded!, false);
            _ = await Assert.That(measured).IsGreaterThan(0L);
            expected += measured;
        }

        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(expected);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(values.Length);
    }
}
