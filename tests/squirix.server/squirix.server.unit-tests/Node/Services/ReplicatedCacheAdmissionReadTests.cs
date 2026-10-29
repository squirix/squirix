using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.QuorumReadTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Conditional writes that memory admission answers without a log record, on the leader of a group elected under quorum reads: the answer
/// comes from a read the group confirmed, so a leader cut off from its followers refuses instead of answering from stale memory.
/// </summary>
public sealed class ReplicatedCacheAdmissionReadTests : ServerUnitTestBase
{
    /// <summary>A leader whose followers confirm its read index answers an add of a stored key false, without appending a log record.</summary>
    /// <param name="getOrAdd">Whether to return the confirmed existing value with the add outcome.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConfirmedLeaderAnswersStoredKeyAdd(bool getOrAdd, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-admission-read-confirmed");
        await using var registry = await OpenAsync(dir, new FakeTimeProvider(), cancellationToken);
        var cache = new StubCache();
        var gateway = new ScriptedGateway();
        await using var committers = Lead(registry, gateway, cache);
        _ = await AuthorizeAsync(registry, committers, cancellationToken);
        await cache.SetEntryAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        var logIndexBefore = await LastLogIndexAsync(registry, cancellationToken);
        using var meter = new Meter("test");
        var admission = CreateAdmission(new ReplicatedCache(cache, committers, true), meter);

        var requestsBefore = gateway.Requests;
        var result = await StartAddAsync(admission, getOrAdd, NewOperationId(), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(result.Added).IsFalse();
        if (getOrAdd)
            _ = await Assert.That(result.Existing.Value).IsEqualTo("b");

        _ = await Assert.That(gateway.Requests - requestsBefore).IsEqualTo(2);
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(logIndexBefore);
    }

    /// <summary>A leader that lost its authority refuses an add of a stored key without sending a request to any follower.</summary>
    /// <param name="getOrAdd">Whether the operation also asks for the existing value.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task NoAuthorityLeaderRefusesStoredKeyAdd(bool getOrAdd, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-admission-read-no-authority");
        await using var registry = await OpenAsync(dir, new FakeTimeProvider(), cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committers = Lead(registry, gateway, cache);
        _ = await AuthorizeAsync(registry, committers, cancellationToken);
        await cache.SetEntryAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        registry.StateFor("n2").BecomeFollower(2UL, false);
        var sentBefore = gateway.Requests;
        using var meter = new Meter("test");
        var admission = CreateAdmission(new ReplicatedCache(cache, committers, true), meter);

        var refusing = StartAddAsync(admission, getOrAdd, NewOperationId(), cancellationToken);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(refusing);

        _ = await Assert.That(gateway.Requests).IsEqualTo(sentBefore);
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }

    /// <summary>
    /// A leader cut off from its followers does not answer an add of a key it stores from its own memory: the refusal is the read quorum failing,
    /// and nothing is appended to the group log.
    /// </summary>
    /// <param name="getOrAdd">Whether the operation also asks for the existing value.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task PartitionedLeaderRefusesStoredKeyAdd(bool getOrAdd, CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-admission-read-add-partitioned");
        await using var registry = await OpenAsync(dir, time, cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committers = Lead(registry, gateway, cache);
        _ = await AuthorizeAsync(registry, committers, cancellationToken);
        await cache.SetEntryAsync(NewOperationId(), CacheName, "b", Entry("b"), cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        var logIndexBefore = await LastLogIndexAsync(registry, cancellationToken);
        using var meter = new Meter("test");
        var admission = CreateAdmission(new ReplicatedCache(cache, committers, true), meter);

        var adding = StartAddAsync(admission, getOrAdd, NewOperationId(), cancellationToken);
        var waited = !adding.IsCompleted;
        time.Advance(ElectionTiming.ElectionTimeout);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(adding.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(waited).IsTrue();
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ReadQuorumUnconfirmedDetail));
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(logIndexBefore);
    }

    /// <summary>A leader cut off from its followers does not answer an update of a key it lacks from its own memory: the refusal is the read quorum failing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PartitionedLeaderRefusesMissingKeyUpdate(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider();
        using var dir = new TempDirectory("squirix-admission-read-update-partitioned");
        await using var registry = await OpenAsync(dir, time, cancellationToken);
        var gateway = new ScriptedGateway();
        var cache = new StubCache();
        await using var committers = Lead(registry, gateway, cache);
        _ = await AuthorizeAsync(registry, committers, cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        var logIndexBefore = await LastLogIndexAsync(registry, cancellationToken);
        using var meter = new Meter("test");
        var admission = CreateAdmission(new ReplicatedCache(cache, committers, true), meter);

        var updating = admission.UpdateAsync(NewOperationId(), CacheName, "b", "c", cancellationToken).AsTask();
        var waited = !updating.IsCompleted;
        time.Advance(ElectionTiming.ElectionTimeout);
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(updating.WaitAsync(HangGuard, TimeProvider.System, cancellationToken));

        _ = await Assert.That(waited).IsTrue();
        _ = await Assert.That((refused.StatusCode, refused.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.ReadQuorumUnconfirmedDetail));
        _ = await Assert.That(await LastLogIndexAsync(registry, cancellationToken)).IsEqualTo(logIndexBefore);
    }

    private static async Task<NodeCacheAddOrGetResult<object?>> StartAddAsync(
        MemoryAdmissionCacheDecorator<object?> admission,
        bool getOrAdd,
        string operationId,
        CancellationToken cancellationToken)
    {
        if (getOrAdd)
            return await admission.AddOrGetAsync(operationId, CacheName, "b", Entry("c"), cancellationToken);

        var added = await admission.TryAddEntryAsync(operationId, CacheName, "b", Entry("c"), cancellationToken);
        return new NodeCacheAddOrGetResult<object?>(added, default);
    }
}
