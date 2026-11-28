using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>
/// The ownership guard refuses every operation on a key of a group this node does not lead with authority and passes the rest through:
/// without elections the ring owner leads statically, with elections the leader view decides the refusal.
/// </summary>
[Immutable]
public sealed class OwnershipGuardCacheDecoratorTests
{
    private const int OperationCount = 9;
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
        static async g => _ = await g.AddOrGetAsync("op", "c", "k", new NodeCacheEntry<string> { Value = "v" }, CancellationToken.None),
        static async g => _ = await g.UpdateAsync("op", "c", "k", "w", CancellationToken.None),
    ];

    /// <summary>A key owned by another node is refused with the stale-owner failure and never reaches the inner cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RemoteKeyIsRefusedBeforeTheInnerCache(CancellationToken cancellationToken)
    {
        var guard = CreateGuard(Remote, ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict).Object);

        for (var operation = 0; operation < OperationCount; operation++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(StartAsync(guard, operation), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

            _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
            _ = await Assert.That(failure.Message).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
            _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        }
    }

    /// <summary>Without elections the refusal carries only the stale-owner error code, as before the leader table existed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaticTableAddsNoLeaderHint(CancellationToken cancellationToken)
    {
        var guard = CreateGuard(Remote, ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict).Object);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(StartAsync(guard, 4), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.Error.Trailers.Count).IsEqualTo(1);
    }

    /// <summary>A leader a higher term deposed refuses every operation, reads included, with stale-term before the inner cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeposedLeaderRefusesStaleTerm(CancellationToken cancellationToken)
    {
        var guard = CreateElectedGuard(new GroupLeaderView(true, false, true, 2, 3, default), ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict).Object);

        for (var operation = 0; operation < OperationCount; operation++)
        {
            var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(StartAsync(guard, operation), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

            _ = await Assert.That((failure.StatusCode, failure.Message)).IsEqualTo((FrameStatus.FailedPrecondition, "stale-term"));
            _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-term");
            _ = await Assert.That(failure.FindTrailer("squirix-leader-node-id")).IsNull();
        }
    }

    /// <summary>A follower that knows the leader refuses as a stale owner naming it in the hint trailers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerRefusesWithLeaderHint(CancellationToken cancellationToken)
    {
        var guard = CreateElectedGuard(new GroupLeaderView(true, false, false, 4, 4, new LeaderRoute(Remote, 4)), ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict).Object);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(StartAsync(guard, 0), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That((failure.StatusCode, failure.Message)).IsEqualTo((FrameStatus.FailedPrecondition, "Key is owned by 'node-b', not current node 'node-a'."));
        _ = await Assert.That(failure.FindTrailer("squirix-error-code")).IsEqualTo("stale-owner");
        _ = await Assert.That(failure.FindTrailer("squirix-leader-node-id")).IsEqualTo(Remote);
        _ = await Assert.That(failure.FindTrailer("squirix-leader-term")).IsEqualTo("4");
    }

    /// <summary>A leader whose leader-term entry is not committed yet refuses retryably: no other leader is known.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PromotionPendingIsUnavailable(CancellationToken cancellationToken)
    {
        var guard = CreateElectedGuard(new GroupLeaderView(true, false, true, 2, 2, default), ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict).Object);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(StartAsync(guard, 2), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That((failure.StatusCode, failure.Message)).IsEqualTo((FrameStatus.Unavailable, ServerOpContract.NoLeaderAuthorityDetail));
    }

    /// <summary>A group this node does not serve is refused as a stale owner naming the ring owner, which serves it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnservedGroupNamesRingOwner(CancellationToken cancellationToken)
    {
        var guard = CreateElectedGuard(default, ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict).Object);

        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(StartAsync(guard, 0), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(failure.Message).IsEqualTo("Key is owned by 'node-b', not current node 'node-a'.");
        _ = await Assert.That(failure.Error.Trailers.Count).IsEqualTo(1);
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
        var expectations = ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict);
        _ = expectations.GetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult<NodeCacheEntry<string>?>(null);
                         });
        _ = expectations.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
                         });
        _ = expectations.RemoveAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.RemoveExpirationAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.CompletedTask;
                         });
        _ = expectations.TouchAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.TryAddEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        _ = expectations.AddOrGetAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(default(NodeCacheAddOrGetResult<string>));
                         });
        _ = expectations.UpdateAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string?>(), Arg.Any<CancellationToken>())
                        .ReturnsAsync((_, _, _, _, _) =>
                         {
                             calls.Value++;
                             return ValueTask.FromResult(false);
                         });
        return expectations.Object;
    }

    private static OwnershipGuardCacheDecorator<string> CreateGuard(string owner, ILogicalNamespacedCache<string> inner)
    {
        var locator = INodeLocator.Mock(MockBehavior.Strict);
        _ = locator.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(owner);
        return new OwnershipGuardCacheDecorator<string>(Self, locator.Object, new StaticLeaderTable(Self), false, inner);
    }

    private static OwnershipGuardCacheDecorator<string> CreateElectedGuard(in GroupLeaderView view, ILogicalNamespacedCache<string> inner)
    {
        var locator = INodeLocator.Mock(MockBehavior.Strict);
        _ = locator.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(Remote);
        var leaders = IGroupLeaderTable.Mock(MockBehavior.Strict);
        _ = leaders.Read(Remote).Returns(view);
        return new OwnershipGuardCacheDecorator<string>(Self, locator.Object, leaders.Object, true, inner);
    }

    private static Task StartAsync(OwnershipGuardCacheDecorator<string> guard, int operation) => Operations[operation](guard);
}
