using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Unit tests for mutating RPC idempotency guard behavior.</summary>
public sealed class RpcMutationIdempotencyGuardTests : UnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Ensures unknown operation ids do not produce a replayed response.</summary>
    [Fact]
    public void TryReplayReturnsFalseWhenOperationIdIsUnknown()
    {
        var guard = new RpcMutationIdempotencyGuard();
        var replayed = guard.TryReplay("op-1", "fp-1", TrySetResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }

    /// <summary>Ensures a recorded success can be replayed from the in-memory cache.</summary>
    [Fact]
    public void RecordSuccessThenTryReplayReturnsCachedResponse()
    {
        var guard = new RpcMutationIdempotencyGuard();
        var original = new TrySetResponse { Added = true };
        guard.RecordSuccess("op-1", "fp-1", original);

        var replayed = guard.TryReplay("op-1", "fp-1", TrySetResponse.Parser, out var response);

        Assert.True(replayed);
        Assert.NotNull(response);
        Assert.True(response.Added);
    }

    /// <summary>Ensures reusing an operation id with a different fingerprint throws a typed exception.</summary>
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

    /// <summary>Ensures empty operation ids are rejected with the stable invalid-argument contract.</summary>
    [Fact]
    public void RequireOperationIdRejectsEmptyValue()
    {
        var ex = Assert.Throws<RpcException>(static () => RpcMutationContracts.RequireOperationId(string.Empty));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdRequiredDetail, ex.Status.Detail);
    }

    /// <summary>Ensures over-length operation ids are rejected before format validation.</summary>
    [Fact]
    public void RequireOperationIdRejectsTooLongValue()
    {
        var tooLong = new string('a', RpcMutationContracts.OperationIdLength + 1);
        var ex = Assert.Throws<RpcException>(() => RpcMutationContracts.RequireOperationId(tooLong));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdTooLongDetail, ex.Status.Detail);
    }

    /// <summary>Ensures malformed operation ids are rejected with the stable format contract.</summary>
    [Fact]
    public void RequireOperationIdRejectsInvalidFormat()
    {
        var ex = Assert.Throws<RpcException>(static () => RpcMutationContracts.RequireOperationId("not-a-valid-operation-id"));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Ensures uppercase hex operation ids are rejected.</summary>
    [Fact]
    public void RequireOperationIdRejectsUppercaseHex()
    {
        var uppercase = ValidOperationId.ToUpperInvariant();
        var ex = Assert.Throws<RpcException>(() => RpcMutationContracts.RequireOperationId(uppercase));

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Ensures conforming operation ids pass validation.</summary>
    [Fact]
    public void RequireOperationIdAcceptsValidValue()
    {
        var normalized = RpcMutationContracts.RequireOperationId(ValidOperationId);
        Assert.Equal(ValidOperationId, normalized);
    }

    /// <summary>Ensures the coordinator replays cached responses without re-executing the handler.</summary>
    [Fact]
    public async Task CoordinatorReplaysWithoutReExecutingHandler()
    {
        var guard = new RpcMutationIdempotencyGuard();
        var coordinator = new RpcMutationIdempotencyCoordinator(guard);
        var executions = 0;
        var entry = new CacheEntry<object?> { Value = "v", Version = 1 }.MapToProto();
        var fingerprint = RpcMutationFingerprints.TrySet("default", "k", entry);

        var first = await coordinator.ExecuteAsync(
            ValidOperationId,
            fingerprint,
            _ =>
            {
                executions++;
                return Task.FromResult(new TrySetResponse { Added = true });
            },
            DefaultCancellationToken);

        var second = await coordinator.ExecuteAsync(
            ValidOperationId,
            fingerprint,
            _ =>
            {
                executions++;
                return Task.FromResult(new TrySetResponse { Added = false });
            },
            DefaultCancellationToken);

        Assert.True(first.Added);
        Assert.True(second.Added);
        Assert.Equal(1, executions);
    }

    /// <summary>Ensures expired idempotency records are swept and no longer replay.</summary>
    [Fact]
    public async Task ExpiredRecordsAreNotReplayed()
    {
        var guard = new RpcMutationIdempotencyGuard(TimeSpan.FromMilliseconds(50));
        guard.RecordSuccess("op-1", "fp-1", new TrySetResponse { Added = true });

        await Task.Delay(100, DefaultCancellationToken);

        var replayed = guard.TryReplay("op-1", "fp-1", TrySetResponse.Parser, out var response);

        Assert.False(replayed);
        Assert.Null(response);
    }
}
