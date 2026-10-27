using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The walk of the node entries by replica group yields each group's stored entries with their absolute deadlines.</summary>
public sealed class ReplicaGroupEntriesTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    /// <summary>The walk of one group yields exactly the entries that group owns, with their deadlines and values.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GroupWalkYieldsOnlyItsEntries(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        await SeedAsync(physical, cancellationToken);
        var entries = new ReplicaGroupEntries(physical, Locator());

        var n1 = await CollectAsync(entries.EnumerateAsync("n1", cancellationToken), cancellationToken);
        var n2 = await CollectAsync(entries.EnumerateAsync("n2", cancellationToken), cancellationToken);

        n1.Sort(StringComparer.Ordinal);
        var ticks = (Start.UtcDateTime + Ttl).Ticks.ToString(CultureInfo.InvariantCulture);
        await SequenceAssert.EqualAsync([$"n1/cache/n1-a/{ticks}", "n1/cache/n1-b/-"], n1, StringComparer.Ordinal);
        await SequenceAssert.EqualAsync([$"n2/cache/n2-a/{ticks}"], n2, StringComparer.Ordinal);
    }

    /// <summary>The walk of every group names the owner of each entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WalkOfAllGroupsNamesOwners(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        await SeedAsync(physical, cancellationToken);

        var all = await CollectAsync(new ReplicaGroupEntries(physical, Locator()).EnumerateAsync(null, cancellationToken), cancellationToken);

        all.Sort(StringComparer.Ordinal);
        _ = await Assert.That(all.Count).IsEqualTo(3);
        _ = await Assert.That((all[0][..2], all[1][..2], all[2][..2])).IsEqualTo(("n1", "n1", "n2"));
    }

    /// <summary>
    /// An entry past its deadline is yielded when expiry is decided by committed records, because the cache keeps it until a record removes it;
    /// the sweep relies on this to find it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastDeadlineEntryKeptWithCommittedExpiry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        await SeedAsync(physical, cancellationToken);
        clock.Advance(Ttl + Ttl);

        var kept = await CollectAsync(new ReplicaGroupEntries(physical, Locator()).EnumerateAsync("n1", cancellationToken), cancellationToken);

        _ = await Assert.That(kept.Count).IsEqualTo(2);
    }

    /// <summary>An entry past its deadline is not yielded when the local clock decides expiry, because the cache already hides it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PastDeadlineEntryHiddenWithLocalExpiry(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(Start);
        var physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>());
        await SeedAsync(physical, cancellationToken);
        clock.Advance(Ttl + Ttl);

        var live = await CollectAsync(new ReplicaGroupEntries(physical, Locator()).EnumerateAsync("n1", cancellationToken), cancellationToken);

        await SequenceAssert.EqualAsync(["n1/cache/n1-b/-"], live, StringComparer.Ordinal);
    }

    private static async Task SeedAsync(PhysicalCache<object?> physical, CancellationToken cancellationToken)
    {
        await physical.SetAsync(new CacheKey(CacheName, "n1-a"), new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await physical.SetAsync(new CacheKey(CacheName, "n1-b"), new NodeCacheEntry<object?>("v"), cancellationToken);
        await physical.SetAsync(new CacheKey(CacheName, "n2-a"), new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
    }

    private static INodeLocator Locator()
    {
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Callback(static (_, key) => key[..2]);
        return locator.Instance();
    }

    private static async Task<List<string>> CollectAsync(IAsyncEnumerable<ReplicaGroupEntry> entries, CancellationToken cancellationToken)
    {
        var lines = new List<string>();
        await foreach (var item in entries.WithCancellation(cancellationToken))
        {
            var ticks = item.Entry.ExpiresUtc is { } deadline ? deadline.Ticks.ToString(CultureInfo.InvariantCulture) : "-";
            lines.Add($"{item.GroupId}/{item.CacheName}/{item.Key}/{ticks}");
        }

        return lines;
    }
}
