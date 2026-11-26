using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// An entry decoded from the journal, a snapshot or the replica log is measurable for every payload kind the codec writes, so the
/// follower apply and the recovery insert, which size the decoded entry, cannot fail on the estimator.
/// </summary>
[Immutable]
public sealed class StoredValueRecoverySizeTests
{
    /// <summary>Every payload kind the entry codec writes decodes into an entry the estimator measures, and the cache records that measure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EveryDecodedPayloadKindIsMeasurable(CancellationToken cancellationToken)
    {
        var estimator = new CacheEntrySizeEstimator<StoredValue>();
        var cache = new PhysicalCache<StoredValue>(TimeProvider.System, estimator);
        ILocalCacheStats stats = cache;
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["tenant"] = "t1" }.ToFrozenDictionary();
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        StoredValue[] values =
        [
            default,
            StoredValue.FromBoolean(true),
            StoredValue.FromText("text"),
            StoredValue.FromBinary([1, 2, 3]),
            StoredValue.FromInteger(42),
            StoredValue.FromDouble(1.5d),
            StoredValue.FromJson(System.Text.Encoding.UTF8.GetBytes("""{"a":[1,"x",true,false,null,{"b":2.5}]}""")),
        ];
        long expected = 0;

        for (var i = 0; i < values.Length; i++)
        {
            var key = new CacheKey("ns", $"k{i}");
            var original = new NodeCacheEntry<StoredValue>(values[i], 3, deadline, null, tags);
            var bytes = JournalEntryPayloadKit.Encode(original);

            var isDecoded = JournalEntryPayload.TryDecode<StoredValue>(bytes, out var decoded, out _);
            _ = await Assert.That(isDecoded).IsTrue();
            await cache.InsertRecoveryAsync(key, decoded!, cancellationToken);

            var measured = estimator.EstimateBytes(key, decoded!, false);
            _ = await Assert.That(measured).IsEqualTo(estimator.EstimateBytes(key, original, false));
            expected += measured;
        }

        _ = await Assert.That(stats.EstimatedBytes).IsEqualTo(expected);
        _ = await Assert.That(stats.EntryCount).IsEqualTo(values.Length);
    }
}
