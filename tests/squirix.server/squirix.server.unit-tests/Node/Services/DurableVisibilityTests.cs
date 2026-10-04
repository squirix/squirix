using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A hosted mutation RPC becomes visible to readers only after its own journal frame is covered by a completed flush.</summary>
[Immutable]
public sealed class DurableVisibilityTests : IsolatedStorageTestBase
{
    private const string Fingerprint = "fp-1";

    private const string OperationId = "0123456789abcdef0123456789abcdef";

    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    private static readonly string KeyA = CacheKey.Default("a").ToString();

    private static readonly string KeyW = CacheKey.Default("w").ToString();

    private readonly Meter _testMeter = new("test");

    /// <summary>While the flush that covers the frame is stalled, the frame is in the file but memory does not hold the value yet.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReaderCannotObserveWriteBeforeFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var target = new Target(journal, CreateStore());
        journal.Writer.Flush.Arm();
        string visibleDuringStall;
        string framesDuringStall;
        TryAddAsyncResponse response;
        try
        {
            var put = target.PutAsync(OperationId, "a", cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            visibleDuringStall = target.Memory.Snapshot;
            framesDuringStall = journal.ReadStampedPuts(cancellationToken);
            journal.Writer.Flush.Release();
            response = await put.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        await journal.ShutdownAsync();

        _ = await Assert.That(framesDuringStall).IsEqualTo(StallableJournal.Describe([$"{KeyA}#{OperationId}", KeyW]));
        _ = await Assert.That(visibleDuringStall).IsEmpty();
        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(target.Memory.Snapshot).IsEqualTo(KeyA);
    }

    /// <inheritdoc />
    protected override void DisposeManaged()
    {
        base.DisposeManaged();
        _testMeter.Dispose();
    }

    private RpcMutationIdempotencyStore CreateStore() => new(new IdempotencyOptions(), "local", new IdempotencyMetrics(_testMeter));

    /// <summary>Creates a journal whose segment header and a first frame are already durable, so later stalls catch only the frames under test.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started journal.</returns>
    private async Task<StallableJournal> CreateWarmJournalAsync(bool groupCommit, CancellationToken cancellationToken)
    {
        var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        try
        {
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("w"), JournalEntryPayloadKit.EncodePut("w"), cancellationToken);
            await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
            return journal;
        }
        catch
        {
            await journal.DisposeAsync();
            throw;
        }
    }

    /// <summary>Idempotent puts through the coordinator, the durable executor and a real (stallable) journal, over an in-memory key model.</summary>
    [ThreadSafe]
    private sealed class Target
    {
        private readonly RpcMutationIdempotencyCoordinator _coordinator;
        private readonly DurableMutationExecutor _executor;
        private readonly StallableJournal _journal;

        internal Target(StallableJournal journal, RpcMutationIdempotencyStore store)
        {
            var log = new EventRecordingLogger();
            _journal = journal;
            _executor = new DurableMutationExecutor(journal.Journal, log);
            _coordinator = new RpcMutationIdempotencyCoordinator(store, journal.Journal, log);
        }

        /// <summary>Gets the keys the mutations applied to memory.</summary>
        internal AppliedKeys Memory { get; } = new();

        internal Task<TryAddAsyncResponse> PutAsync(string operationId, string key, CancellationToken cancellationToken) =>
            _coordinator.ExecuteAsync(
                operationId,
                Fingerprint,
                (Target: this, Key: key),
                static async (s, ct) =>
                {
                    var applied = await s.Target.Memory.PutAsync(s.Target._executor, s.Target._journal.Journal, s.Key, ct).ConfigureAwait(false);
                    return new TryAddAsyncResponse { Added = applied == 1 };
                },
                cancellationToken);
    }
}
