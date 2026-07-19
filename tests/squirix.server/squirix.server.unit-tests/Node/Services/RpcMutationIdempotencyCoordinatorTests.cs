using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for mutating RPC idempotency store behavior.</summary>
public sealed class RpcMutationIdempotencyCoordinatorTests : ServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Ensures the coordinator replays cached responses without re-executing the handler.</summary>
    [Fact]
    public async Task CoordinatorReplaysWithoutReExecutingHandler()
    {
        var store = new RpcMutationIdempotencyStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store);
        var ctx = new ExecutionCounter();

        var first = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            ctx,
            static (state, _) =>
            {
                state.Value++;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            DefaultCancellationToken);

        var second = await coordinator.ExecuteAsync(
            ValidOperationId,
            "fingerprint",
            ctx,
            static (state, _) =>
            {
                state.Value++;
                return Task.FromResult(new TryAddAsyncResponse { Added = false });
            },
            DefaultCancellationToken);

        Assert.True(first.Added);
        Assert.True(second.Added);
        Assert.Equal(1, ctx.Value);
    }

    /// <summary>Ensures expired idempotency records are swept and no longer replay.</summary>
    [Fact]
    public async Task ExpiredRecordsAreNotReplayed()
    {
        var store = new RpcMutationIdempotencyStore(TimeSpan.FromMilliseconds(50));
        store.RecordSuccess("op-1", "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        await Task.Delay(100, DefaultCancellationToken);

        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>Ensures a recorded success can be replayed from the in-memory cache.</summary>
    [Fact]
    public void RecordSuccessThenTryReplayReturnsCachedResponse()
    {
        var store = new RpcMutationIdempotencyStore();
        var original = new TryAddAsyncResponse { Added = true };
        store.RecordSuccess("op-1", "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(original));

        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <summary>Ensures conforming operation ids pass validation.</summary>
    [Fact]
    public void RequireOperationIdAcceptsValidValue()
    {
        var normalized = RpcMutationContracts.RequireOperationId(ValidOperationId);
        Assert.Equal(ValidOperationId, normalized);
    }

    /// <summary>Ensures empty operation ids are rejected with the stable invalid-argument contract.</summary>
    [Fact]
    public void RequireOperationIdRejectsEmptyValue()
    {
        var ex = Assert.Throws<RpcException>(static () => _ = RpcMutationContracts.RequireOperationId(string.Empty));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdRequiredDetail, ex.Status.Detail);
    }

    /// <summary>Ensures malformed operation ids are rejected with the stable format contract.</summary>
    [Fact]
    public void RequireOperationIdRejectsInvalidFormat()
    {
        var ex = Assert.Throws<RpcException>(static () => _ = RpcMutationContracts.RequireOperationId("not-a-valid-operation-id"));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Ensures over-length operation ids are rejected before format validation.</summary>
    [Fact]
    public void RequireOperationIdRejectsTooLongValue()
    {
        var tooLong = new string('a', RpcMutationContracts.OperationIdLength + 1);
        var ex = Assert.Throws<RpcException>(() => _ = RpcMutationContracts.RequireOperationId(tooLong));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdTooLongDetail, ex.Status.Detail);
    }

    /// <summary>Ensures uppercase hex operation ids are rejected.</summary>
    [Fact]
    public void RequireOperationIdRejectsUppercaseHex()
    {
        var uppercase = ValidOperationId.ToUpperInvariant();
        var ex = Assert.Throws<RpcException>(() => _ = RpcMutationContracts.RequireOperationId(uppercase));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Ensures RestoreRecord honors CreatedUtc for retention sweeps after recovery replay.</summary>
    [Fact]
    public void RestoredExpiredRecordIsNotReplayed()
    {
        var store = new RpcMutationIdempotencyStore(TimeSpan.FromMinutes(15));
        var responseBytes = RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        store.RestoreRecord("op-1", "fp-1", responseBytes, DateTime.UtcNow.AddMinutes(-20));

        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>Ensures reusing an operation id with a different fingerprint throws a typed exception.</summary>
    [Fact]
    public void ReuseWithDifferentFingerprintThrowsTypedException()
    {
        var store = new RpcMutationIdempotencyStore();
        store.RecordSuccess("op-1", "fp-1", RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true }));

        var ex = Assert.Throws<ServerOpIdMismatchException>(() =>
        {
            var replayed = store.TryReplay("op-1", "fp-2", TryAddAsyncResponse.Parser, out var replay);
            Assert.Fail($"Expected reuse mismatch, got replayed={replayed}, replay={replay}");
        });

        Assert.Equal(ServerOpIdMismatchException.StableDetail, ex.Message);
    }

    /// <summary>Ensures unknown operation ids do not produce a replayed response.</summary>
    [Fact]
    public void ReplayReturnsFalseWhenOperationIdIsUnknown()
    {
        var store = new RpcMutationIdempotencyStore();
        var replayed = store.TryReplay("op-1", "fp-1", TryAddAsyncResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    private sealed class ExecutionCounter
    {
        internal int Value { get; set; }
    }
}
