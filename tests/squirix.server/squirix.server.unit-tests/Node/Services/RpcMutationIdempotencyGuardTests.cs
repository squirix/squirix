using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Coordinator and journal integration for durable idempotency.</summary>
public sealed class RpcMutationIdempotencyGuardTests : ServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Ensures unknown operation ids do not produce a replayed response.</summary>
    [Fact]
    public async Task CoordinatorWithJournalPersistsOutcomeOnExecute()
    {
        using var dir = new TempDirectory("squirix-coordinator-journal-outcome");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
        };

        using var manifestStore = new ManifestStore(options);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            options,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);

        var store = new RpcMutationIdempotencyStore();
        var coordinator = new RpcMutationIdempotencyCoordinator(store, journal);
        var key = CacheKey.Default("guard-key");
        var payload = JournalEntryPayloadKit.EncodePut("v");
        var executor = new DurableMutationExecutor(journal);

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
                var added = await state.Executor.ExecuteAsync(
                    null,
                    static _ => new ValueTask<DurableMutationCondition<bool>>(DurableMutationCondition<bool>.Apply()),
                    new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload), bool>(
                        (state.Journal, state.Key, state.Payload),
                        static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                        static (_, _) => new ValueTask<bool>(true)),
                    cancellationToken).ConfigureAwait(false);
                return new TryAddAsyncResponse { Added = added };
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

            Assert.Equal(ValidOperationId, record.IdempotencyOperationId);
            found = true;
        }

        Assert.True(found);
    }
}
