using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>
/// The ownership guard refuses every operation on a key of a group this node does not lead with authority and passes the rest through:
/// without elections the ring owner leads statically, with elections the leader view decides the refusal.
/// </summary>
[Immutable]
public sealed class OwnershipGuardCacheDecoratorTests
{
    private const int OperationCount = 8;
    private const string Remote = "node-b";
    private const string Self = "node-a";

    private static readonly Func<OwnershipGuardCacheDecorator<string>, Task>[] Operations =
    [
        static async g => _ = await g.GetEntryAsync("c", "k", CancellationToken.None),
        static async g => _ = await g.GetValueAsync("c", "k", CancellationToken.None),
        static async g => _ = await g.RemoveAsync("op", "c", "k", CancellationToken.None),
        static async g => _ = await g.RemoveExpirationAsync("op", "c", "k", CancellationToken.None),
        static async g => await g.SetEntryAsync("op", "c", "k", new NodeCacheEntry<string> { Value = "v" }, CancellationToken.None),
        static async g => _ = await g.TouchAsync("op", "c", "k", TimeSpan.FromMinutes(1), CancellationToken.None),
        static async g => _ = await g.TryAddEntryAsync("op", "c", "k", new NodeCacheEntry<string> { Value = "v" }, CancellationToken.None),
        static async g => _ = await g.UpdateAsync("op", "c", "k", "w", CancellationToken.None),
    ];

    /// <summary>A key owned by another node is refused with the stale-owner failure and never reaches the inner cache.</summary>
    [Test]
    public async Task RemoteKeyIsRefusedBeforeTheInnerCache()
    {
        var guard = CreateGuard(Remote, new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

        for (var operation = 0; operation < OperationCount; operation++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(StartAsync(guard, operation));

            _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
            _ = await Assert.That(failure.Status.Detail).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
            _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        }
    }

    /// <summary>Without elections the refusal carries only the stale-owner error code, as before the leader table existed.</summary>
    [Test]
    public async Task StaticTableAddsNoLeaderHint()
    {
        var guard = CreateGuard(Remote, new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(StartAsync(guard, 4));

        _ = await Assert.That(failure.Trailers.Count).IsEqualTo(1);
    }

    /// <summary>A leader a higher term deposed refuses every operation, reads included, with stale-term before the inner cache.</summary>
    [Test]
    public async Task DeposedLeaderRefusesStaleTerm()
    {
        var guard = CreateElectedGuard(new GroupLeaderView(true, false, true, 2, 3, default), new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

        for (var operation = 0; operation < OperationCount; operation++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(StartAsync(guard, operation));

            _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "stale-term"));
            _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-term");
            _ = await Assert.That(failure.Trailers.Get("squirix-leader-node-id")).IsNull();
        }
    }

    /// <summary>A follower that knows the leader refuses as a stale owner naming it in the hint trailers.</summary>
    [Test]
    public async Task FollowerRefusesWithLeaderHint()
    {
        var guard = CreateElectedGuard(new GroupLeaderView(true, false, false, 4, 4, new LeaderRoute(Remote, 4)), new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(StartAsync(guard, 0));

        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.FailedPrecondition, "Key is owned by 'node-b', not current node 'node-a'."));
        _ = await Assert.That(failure.Trailers.GetValue("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(failure.Trailers.GetValue("squirix-leader-node-id")).IsEqualTo(Remote);
        _ = await Assert.That(failure.Trailers.GetValue("squirix-leader-term")).IsEqualTo("4");
    }

    /// <summary>A leader whose leader-term entry is not committed yet refuses retryably: no other leader is known.</summary>
    [Test]
    public async Task PromotionPendingIsUnavailable()
    {
        var guard = CreateElectedGuard(new GroupLeaderView(true, false, true, 2, 2, default), new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(StartAsync(guard, 2));

        _ = await Assert.That((failure.StatusCode, failure.Status.Detail)).IsEqualTo((StatusCode.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }

    /// <summary>A group this node does not serve is refused as a stale owner naming the ring owner, which serves it.</summary>
    [Test]
    public async Task UnservedGroupNamesRingOwner()
    {
        var guard = CreateElectedGuard(default, new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(StartAsync(guard, 0));

        _ = await Assert.That(failure.Status.Detail).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
        _ = await Assert.That(failure.Trailers.Count).IsEqualTo(1);
    }

    /// <summary>A key of a group this node leads with authority reaches the inner cache, whichever node owns it on the ring.</summary>
    [Test]
    public async Task AuthorizedLeaderServesForeignGroup()
    {
        var calls = new StrongBox<int>();
        var guard = CreateElectedGuard(new GroupLeaderView(true, true, false, 5, 5, new LeaderRoute(Self, 5)), CreateCountingInner(calls));

        for (var operation = 0; operation < OperationCount; operation++)
            await StartAsync(guard, operation);

        _ = await Assert.That(calls.Value).IsEqualTo(OperationCount);
    }

    /// <summary>A key this node owns reaches the inner cache for every operation.</summary>
    [Test]
    public async Task LocalKeyReachesTheInnerCache()
    {
        var calls = new StrongBox<int>();
        var guard = CreateGuard(Self, CreateCountingInner(calls));

        for (var operation = 0; operation < OperationCount; operation++)
            await StartAsync(guard, operation);

        _ = await Assert.That(calls.Value).IsEqualTo(OperationCount);
    }

    private static ILogicalNamespacedCache<string> CreateCountingInner(StrongBox<int> calls)
    {
        var expectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = expectations.Setups.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult<NodeCacheEntry<string>?>(null);
                         });
        _ = expectations.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
                         });
        _ = expectations.Setups.RemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(new CacheRemoveResult<string>(false, null));
                         });
        _ = expectations.Setups.RemoveExpirationAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.CompletedTask;
                         });
        _ = expectations.Setups.TouchAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.Setups.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.Setups.UpdateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                        .Callback((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        return expectations.Instance();
    }

    private static OwnershipGuardCacheDecorator<string> CreateGuard(string owner, ILogicalNamespacedCache<string> inner)
    {
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(owner);
        return new OwnershipGuardCacheDecorator<string>(Self, locator.Instance(), new StaticLeaderTable(Self), false, inner);
    }

    private static OwnershipGuardCacheDecorator<string> CreateElectedGuard(in GroupLeaderView view, ILogicalNamespacedCache<string> inner)
    {
        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Remote);
        var leaders = new IGroupLeaderTableCreateExpectations();
        _ = leaders.Setups.Read(Remote).ReturnValue(view);
        return new OwnershipGuardCacheDecorator<string>(Self, locator.Instance(), leaders.Instance(), true, inner);
    }

    private static Task StartAsync(OwnershipGuardCacheDecorator<string> guard, int operation) => Operations[operation](guard);
}
