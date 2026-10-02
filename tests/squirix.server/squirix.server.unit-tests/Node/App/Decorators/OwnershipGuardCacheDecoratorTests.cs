using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>The ownership guard refuses every operation on a key another node owns and passes the rest through.</summary>
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
        return new OwnershipGuardCacheDecorator<string>(Self, locator.Instance(), inner);
    }

    private static Task StartAsync(OwnershipGuardCacheDecorator<string> guard, int operation) => Operations[operation](guard);
}
