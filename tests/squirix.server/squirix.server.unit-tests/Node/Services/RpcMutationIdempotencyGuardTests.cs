using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Unit tests for mutating RPC idempotency guard behavior.
/// </summary>
public sealed class RpcMutationIdempotencyGuardTests
{
    /// <summary>
    /// Ensures unknown operation ids do not produce a replayed response.
    /// </summary>
    [Fact]
    public void TryReplayReturnsFalseWhenOperationIdIsUnknown()
    {
        var guard = new RpcMutationIdempotencyGuard();
        var replayed = guard.TryReplay("op-1", "fp-1", TrySetResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>
    /// Ensures a recorded success can be replayed from the in-memory cache.
    /// </summary>
    [Fact]
    public void RecordSuccessThenTryReplayReturnsCachedResponse()
    {
        var guard = new RpcMutationIdempotencyGuard();
        var original = new TrySetResponse { Added = true };
        guard.RecordSuccess("op-1", "fp-1", original);

        var replayed = guard.TryReplay("op-1", "fp-1", TrySetResponse.Parser, out var response);

        Assert.True(replayed);
        Assert.True(response.Added);
    }

    /// <summary>
    /// Ensures reusing an operation id with a different fingerprint throws a typed exception.
    /// </summary>
    [Fact]
    public void ReuseWithDifferentFingerprintThrowsTypedException()
    {
        var guard = new RpcMutationIdempotencyGuard();
        guard.RecordSuccess("op-1", "fp-1", new TrySetResponse { Added = true });

        var ex = Assert.Throws<OperationIdReuseMismatchException>(() =>
        {
            var replayed = guard.TryReplay("op-1", "fp-2", TrySetResponse.Parser, out var replay);
            Assert.Fail($"Expected reuse mismatch, got replayed={replayed}, replay={replay}");
        });

        Assert.Equal(OperationIdReuseMismatchException.StableDetail, ex.Message);
    }

    /// <summary>
    /// Ensures empty operation ids are rejected with the stable invalid-argument contract.
    /// </summary>
    [Fact]
    public void RequireOperationIdRejectsEmptyValue()
    {
        var ex = Assert.Throws<RpcException>(static () => RpcMutationContracts.RequireOperationId(string.Empty));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdRequiredDetail, ex.Status.Detail);
    }

    /// <summary>
    /// Ensures the coordinator replays cached responses without re-executing the handler.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task CoordinatorReplaysWithoutReExecutingHandler()
    {
        var guard = new RpcMutationIdempotencyGuard();
        var coordinator = new RpcMutationIdempotencyCoordinator(guard);
        var executions = 0;
        var entry = new CacheEntry<object?> { Value = "v", Version = 1 }.MapToProto();
        var fingerprint = RpcMutationFingerprints.TrySet("default", "k", entry);

        var first = await coordinator.ExecuteAsync(
            "op-1",
            fingerprint,
            _ =>
            {
                executions++;
                return Task.FromResult(new TrySetResponse { Added = true });
            },
            TestContext.Current.CancellationToken);

        var second = await coordinator.ExecuteAsync(
            "op-1",
            fingerprint,
            _ =>
            {
                executions++;
                return Task.FromResult(new TrySetResponse { Added = false });
            },
            TestContext.Current.CancellationToken);

        Assert.True(first.Added);
        Assert.True(second.Added);
        Assert.Equal(1, executions);
    }

    /// <summary>
    /// Ensures expired idempotency records are swept and no longer replay.
    /// </summary>
    /// <returns>A task representing the asynchronous test.</returns>
    [Fact]
    public async Task ExpiredRecordsAreNotReplayed()
    {
        var guard = new RpcMutationIdempotencyGuard(TimeSpan.FromMilliseconds(50));
        guard.RecordSuccess("op-1", "fp-1", new TrySetResponse { Added = true });

        await Task.Delay(100, TestContext.Current.CancellationToken);

        var replayed = guard.TryReplay("op-1", "fp-1", TrySetResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }
}
