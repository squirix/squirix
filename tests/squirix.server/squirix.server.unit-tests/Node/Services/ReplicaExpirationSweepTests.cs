using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The leader sweep expires the owned keys no read touches, through committed tombstones, and nothing else.</summary>
public sealed class ReplicaExpirationSweepTests : ServerUnitTestBase
{
    private const string CacheName = "cache";

    /// <summary>The event id of a failed sweep pass.</summary>
    private const int SweepFailedEventId = 4033;

    /// <summary>The event id of a sweep stopped by the host.</summary>
    private const int SweepStoppedEventId = 4034;

    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

    /// <summary>A pass stops after the most keys it may expire; the next pass expires the rest.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PassStopsAtMaxPerPass(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-sweep-max");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), new ClientCache<object?>(physical, physical), clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "a", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "b", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "c", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        clock.Advance(Ttl);
        using var sweep = CreateSweep(committer, physical, new EventRecordingLogger());

        var first = await sweep.SweepOnceAsync(cancellationToken);
        var second = await sweep.SweepOnceAsync(cancellationToken);

        _ = await Assert.That((first, second)).IsEqualTo((2, 1));
        ILocalCacheStats stats = physical;
        _ = await Assert.That(stats.EntryCount).IsEqualTo(0);
    }

    /// <summary>A pass expires the owned keys past their deadline and leaves live keys and keys of followed groups in place.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepExpiresOwnedExpiredKeysOnly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-sweep-owned");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), new ClientCache<object?>(physical, physical), clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "expired", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v"), cancellationToken);
        await physical.SetAsync(new CacheKey(CacheName, "followed"), new NodeCacheEntry<object?>("v", 1, Start.UtcDateTime), cancellationToken);
        clock.Advance(Ttl);

        using var sweep = CreateSweep(committer, physical, new EventRecordingLogger());
        var expired = await sweep.SweepOnceAsync(cancellationToken);

        _ = await Assert.That(expired).IsEqualTo(1);
        _ = await Assert.That(await RawAsync(physical, "expired", cancellationToken)).IsNull();
        _ = await Assert.That(await RawAsync(physical, "live", cancellationToken)).IsNotNull();
        _ = await Assert.That(await RawAsync(physical, "followed", cancellationToken)).IsNotNull();
    }

    /// <summary>
    /// A node leading two groups expires the keys of each through the committer of its group, in one pass, and leaves the key of a group it
    /// only follows in place.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepExpiresEachLedGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-sweep-led");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        var local = new ClientCache<object?>(physical, physical);
        await using var committers = LeadTwo(registry, (new ScriptedGateway(), new ScriptedGateway()), local, clock);
        await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await physical.SetAsync(new CacheKey(CacheName, "c"), new NodeCacheEntry<object?>("v", 1, Start.UtcDateTime), cancellationToken);
        clock.Advance(Ttl);
        using var sweep = new ReplicaExpirationSweepService(committers, physical, Owners(), new EventRecordingLogger());

        var expired = await sweep.SweepOnceAsync(cancellationToken);

        _ = await Assert.That(expired).IsEqualTo(2);
        _ = await Assert.That((await RawAsync(physical, "a", cancellationToken), await RawAsync(physical, "b", cancellationToken))).IsEqualTo((null, null));
        _ = await Assert.That(await RawAsync(physical, "c", cancellationToken)).IsNotNull();
        await SequenceAssert.EqualAsync(["a", "a"], await KeysAsync(registry, "n1", cancellationToken), StringComparer.Ordinal);
        await SequenceAssert.EqualAsync(["b", "b"], await KeysAsync(registry, "n2", cancellationToken), StringComparer.Ordinal);
    }

    /// <summary>A pass whose tombstone cannot commit stops there, keeps the entries, and logs one failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepStopsOnRefusal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-sweep-refused");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        var gateway = new ScriptedGateway();
        await using var committer = CreateCommitter(registry, gateway, new ClientCache<object?>(physical, physical), clock);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "a", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await committer.CommitSetAsync(NewOperationId(), CacheName, "b", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        clock.Advance(Ttl);
        var log = new EventRecordingLogger();

        using var sweep = CreateSweep(committer, physical, log);
        var expired = await sweep.SweepOnceAsync(cancellationToken);

        _ = await Assert.That(expired).IsEqualTo(0);
        _ = await Assert.That(log.Count(SweepFailedEventId)).IsEqualTo(1);
        _ = await Assert.That(await RawAsync(physical, "a", cancellationToken)).IsNotNull();
        _ = await Assert.That(await RawAsync(physical, "b", cancellationToken)).IsNotNull();
    }

    /// <summary>
    /// A led group whose tombstone cannot commit is logged once and skipped for the rest of the pass, so the expired key of the other led
    /// group is still removed.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedGroupDoesNotStopOtherGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-sweep-isolated");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        var ofN1 = new ScriptedGateway();
        await using var committers = LeadTwo(registry, (ofN1, new ScriptedGateway()), new ClientCache<object?>(physical, physical), clock);
        await committers.For("n1").CommitSetAsync(NewOperationId(), CacheName, "a", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        await committers.For("n2").CommitSetAsync(NewOperationId(), CacheName, "b", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        ofN1.Set("n2", FollowerMode.Down);
        ofN1.Set("n3", FollowerMode.Down);
        clock.Advance(Ttl);
        var log = new EventRecordingLogger();
        using var sweep = new ReplicaExpirationSweepService(committers, physical, Owners(), log);

        var expired = await sweep.SweepOnceAsync(cancellationToken);

        _ = await Assert.That(expired).IsEqualTo(1);
        _ = await Assert.That(log.Count(SweepFailedEventId)).IsEqualTo(1);
        _ = await Assert.That(await RawAsync(physical, "a", cancellationToken)).IsNotNull();
        _ = await Assert.That(await RawAsync(physical, "b", cancellationToken)).IsNull();
    }

    /// <summary>A sweep stopped by the host ends without a failure and logs one shutdown line.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SweepStopsCleanly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-expiry-sweep-stop");
        var clock = new DueTimerClock(ReplicaExpirationSweepService.Interval);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        var physical = new PhysicalCache<object?>(clock, expiry: CacheExpiryAuthority.CommittedRecords);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), new ClientCache<object?>(physical, physical), clock);
        var log = new EventRecordingLogger();
        using var sweep = CreateSweep(committer, physical, log);

        await sweep.StartAsync(cancellationToken);

        // The host starts the loop in the background: stop it only once it waits for its first pass.
        _ = await Assert.That(await clock.TimerCreated.WaitAsync(TimeSpan.FromSeconds(10), cancellationToken)).IsTrue();
        await sweep.StopAsync(cancellationToken);

        _ = await Assert.That(log.Count(SweepStoppedEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Count(SweepFailedEventId)).IsEqualTo(0);
    }

    private static ReplicaExpirationSweepService CreateSweep(ReplicaGroupCommitter committer, PhysicalCache<object?> physical, EventRecordingLogger log)
    {
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Callback(static (_, key) => string.Equals(key, "followed", StringComparison.Ordinal) ? "n2" : "n1");
        return new ReplicaExpirationSweepService(LeadOwn(committer), physical, locator.Instance(), log) { MaxPerPass = 2 };
    }

    private static ValueTask<NodeCacheEntry<object?>?> RawAsync(PhysicalCache<object?> physical, string key, CancellationToken cancellationToken) =>
        physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, key), cancellationToken);
}
