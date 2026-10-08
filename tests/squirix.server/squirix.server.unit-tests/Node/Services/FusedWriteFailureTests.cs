using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An idempotent write whose outcome frame is on the ring ends in an unknown outcome on any failure, never in a second outcome frame or a lost record.</summary>
[Immutable]
public sealed class FusedWriteFailureTests : IsolatedStorageTestBase
{
    private const string Fingerprint = "fp";

    /// <summary>A memory apply that fails after the fused outcome frame was enqueued is an unknown outcome; the record stays unconfirmed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyFailureIsUnknown(CancellationToken cancellationToken)
    {
        var inner = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = inner.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Throws(new IOException("apply failed"));
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal, inner.Instance());
        var operationId = FusedWriteHarness.OpId(1);

        var first = RunAsync(harness, operationId, () => SetAsync(harness, operationId, "a"), cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(first);
        var retry = await NodeAsyncAssert.ThrowsAsync<RpcException>(RunAsync(harness, operationId, static () => Task.CompletedTask, cancellationToken));

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(retry.Status.Detail)).IsTrue();
        _ = await Assert.That(harness.Store.TryReplay(operationId, Fingerprint, SetAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>A snapshot cut waits for a parked fused write; when the flush then fails after both frames were written, the write is unknown, nothing is applied, and the cut fails.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CutWhileParkedFailsWithFlush(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);
        journal.Writer.Flush.Arm();

        var write = harness.SetThroughScopeAsync(operationId, "a", cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        var cut = journal.Journal.ExecuteSnapshotCutAsync(0, static (_, _, _) => ValueTask.FromResult(0), static (_, _, _, _) => ValueTask.FromResult(0), cancellationToken)
                         .AsTask();
        journal.Writer.Flush.ReleaseWithFailure(new IOException("fsync failed"));
        var writeError = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(write);
        var cutError = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(cut);

        _ = await Assert.That(writeError).IsNotNull();
        _ = await Assert.That(cutError).IsNotNull();
        _ = await Assert.That((await harness.Physical.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).Found).IsFalse();
    }

    /// <summary>A projection that fails or answers another type leaves the write unfused: one outcome frame after the apply, two flushes, and a retry replays it.</summary>
    /// <param name="wrongType">Whether the projection answers the wrong response type instead of throwing.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FailedProjectionFallsBackToOneOutcome(bool wrongType, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        var flushesBefore = harness.FlushCount;
        _ = await RunAsync(
            harness,
            operationId,
            () =>
            {
                RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(_ =>
                    wrongType ? new TouchAsyncResponse() : throw new InvalidOperationException("projection failed"));
                return harness.Cache.SetEntryAsync(operationId, FusedWriteHarness.CacheName, FusedWriteHarness.Key, new NodeCacheEntry<object?>("a"), cancellationToken).AsTask();
            },
            cancellationToken);
        var flushes = harness.FlushCount - flushesBefore;
        var replayed = await RunAsync(harness, operationId, static () => throw new InvalidOperationException("the retry must not run"), cancellationToken);
        await journal.ShutdownAsync();
        var outcomes = harness.ReadFrames(cancellationToken).FindAll(static frame => frame.Operation == JournalOperationKind.IdempotencyOutcome);

        _ = await Assert.That(flushes).IsEqualTo(2);
        _ = await Assert.That(outcomes).Count().IsEqualTo(1);
        _ = await Assert.That(replayed).IsEqualTo(new SetAsyncResponse());
        _ = await Assert.That((await harness.Physical.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).Found).IsTrue();
    }

    /// <summary>A failure after the outcome frame reached the ring is an unknown outcome, appends no second outcome frame and latches the journal, so memory never lacks what the journal flushes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FaultAfterEnqueueAppendsNoFallback(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);
        var armed = new StrongBox<bool>();
        var subscriber = new FailingSubscriber(armed);
        journal.Journal.OnAppended += subscriber.OnAppended;

        try
        {
            var run = RunAsync(
                harness,
                operationId,
                () =>
                {
                    RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(_ =>
                    {
                        Volatile.Write(ref armed.Value, true);
                        return new SetAsyncResponse();
                    });
                    return harness.Cache.SetEntryAsync(operationId, FusedWriteHarness.CacheName, FusedWriteHarness.Key, new NodeCacheEntry<object?>("a"), cancellationToken)
                                  .AsTask();
                },
                cancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(run);
            await journal.ShutdownAsync();
            var outcomes = harness.ReadFrames(cancellationToken).FindAll(static frame => frame.Operation == JournalOperationKind.IdempotencyOutcome);
            var exported = Export(harness);

            _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
            _ = await Assert.That(outcomes.Count).IsLessThanOrEqualTo(1);
            _ = await Assert.That(journal.Journal.GetJournalThreadFailure()).IsNotNull();
            _ = await Assert.That((await harness.Physical.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).Found).IsFalse();
            _ = await Assert.That((await Assert.That(exported).HasSingleItem()).State).IsEqualTo(IdempotencyRecordState.Completed);
        }
        finally
        {
            journal.Journal.OnAppended -= subscriber.OnAppended;
        }
    }

    /// <summary>A write ack that faults for the outcome frame, with group commit on, is an unknown outcome: nothing is applied and no fallback frame follows.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FaultedOutcomeAckIsUnknown(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, true, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        // The mutation frame's write passes; the outcome frame's write blocks and then fails, which faults its write ack after it was enqueued.
        journal.Writer.Write.ArmAfter(1);
        var write = RunAsync(harness, operationId, () => SetAsync(harness, operationId, "a"), cancellationToken);
        await journal.Writer.Write.Entered.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        journal.Writer.Write.ReleaseWithFailure(new IOException("outcome write failed"));
        var error = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(write);

        _ = await Assert.That(
            error is SquirixException { Code: SquirixErrorCode.CommitOutcomeUnknown } ||
            (error is RpcException rpc && ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(rpc.Status.Detail))).IsTrue();
        _ = await Assert.That((await harness.Physical.GetValueAsync(new CacheKey(FusedWriteHarness.CacheName, FusedWriteHarness.Key), cancellationToken)).Found).IsFalse();
        _ = await Assert.That(harness.Store.TryReplay(operationId, Fingerprint, SetAsyncResponse.Parser, out _)).IsFalse();
    }

    /// <summary>A failure after the apply, above the executor, is unknown to the first caller, and the retry replays the recorded success.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PostApplyFailureThenRetryReplays(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        var first = RunAsync(
            harness,
            operationId,
            async () =>
            {
                await SetAsync(harness, operationId, "a");
                throw new InvalidOperationException("response building failed");
            },
            cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(first);
        var replayed = await RunAsync(harness, operationId, static () => throw new InvalidOperationException("the retry must not run"), cancellationToken);
        var exported = Export(harness);

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(replayed).IsEqualTo(new SetAsyncResponse());
        _ = await Assert.That((await Assert.That(exported).HasSingleItem()).State).IsEqualTo(IdempotencyRecordState.Completed);
    }

    /// <summary>A journal that refuses the outcome frame between the two appends leaves no outcome frame, and the write is an unknown outcome.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RefusedOutcomeLeavesNoOutcomeFrame(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, groupCommit, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        var run = RunAsync(
            harness,
            operationId,
            () =>
            {
                // The projection runs right before the outcome append: latching the journal there refuses that append and the wait for it.
                RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(_ =>
                {
                    journal.Journal.FailJournalPipeline(new IOException("journal latched"));
                    return new SetAsyncResponse();
                });
                return harness.Cache.SetEntryAsync(operationId, FusedWriteHarness.CacheName, FusedWriteHarness.Key, new NodeCacheEntry<object?>("a"), cancellationToken).AsTask();
            },
            cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(run);
        await journal.ShutdownAsync();
        var outcomes = harness.ReadFrames(cancellationToken).FindAll(static frame => frame.Operation == JournalOperationKind.IdempotencyOutcome);

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(outcomes).IsEmpty();
    }

    /// <summary>A second cache mutation frame in a scope that appended its outcome is refused before it is enqueued, and the write is an unknown outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondMutationAfterOutcomeRefused(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        var run = RunAsync(
            harness,
            operationId,
            async () =>
            {
                await SetAsync(harness, operationId, "a");
                await harness.Cache.SetEntryAsync(operationId, FusedWriteHarness.CacheName, "other", new NodeCacheEntry<object?>("b"), cancellationToken);
            },
            cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(run);
        var replayed = await RunAsync(harness, operationId, static () => throw new InvalidOperationException("the retry must not run"), cancellationToken);
        await journal.ShutdownAsync();
        var frames = harness.ReadFrames(cancellationToken);

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(frames).Count().IsEqualTo(2);
        _ = await Assert.That(frames[1].Operation).IsEqualTo(JournalOperationKind.IdempotencyOutcome);

        // The first write was already recorded as completed, so the refusal latches the journal: the retry replays that success, and nothing
        // more can be written behind it.
        _ = await Assert.That(replayed).IsEqualTo(new SetAsyncResponse());
        _ = await Assert.That(journal.Journal.GetJournalThreadFailure()).IsNotNull();
    }

    /// <summary>A write whose durable source is not the cache journal is not fused: it waits once, after its outcome frame is appended the usual way.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SuspendedStampingIsNotFused(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        var flushesBefore = harness.FlushCount;
        _ = await RunAsync(
            harness,
            operationId,
            async () =>
            {
                RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static _ => new SetAsyncResponse());
                using var suspended = RpcMutationIdempotencyExecutionAmbient.SuspendStamping();
                await harness.Cache.SetEntryAsync(operationId, FusedWriteHarness.CacheName, FusedWriteHarness.Key, new NodeCacheEntry<object?>("a"), cancellationToken);
            },
            cancellationToken);
        var flushes = harness.FlushCount - flushesBefore;
        await journal.ShutdownAsync();
        var frames = harness.ReadFrames(cancellationToken);

        _ = await Assert.That(flushes).IsEqualTo(1);
        _ = await Assert.That(frames).Count().IsEqualTo(2);
        _ = await Assert.That(frames[0].MutationOperationId).IsNull();
        _ = await Assert.That(frames[1].Operation).IsEqualTo(JournalOperationKind.IdempotencyOutcome);
    }

    /// <summary>A mutation frame that is not stamped is refused after the outcome frame too.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnstampedMutationAfterOutcomeRefused(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var harness = new FusedWriteHarness(Dir, journal);
        var operationId = FusedWriteHarness.OpId(1);

        var run = RunAsync(
            harness,
            operationId,
            async () =>
            {
                await SetAsync(harness, operationId, "a");
                using var suspended = RpcMutationIdempotencyExecutionAmbient.SuspendStamping();
                await journal.Journal.AppendPutUnderGateAsync(new CacheKey(FusedWriteHarness.CacheName, "other"), JournalEntryPayloadKit.EncodePut("b"), cancellationToken);
            },
            cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(run);
        await journal.ShutdownAsync();
        var frames = harness.ReadFrames(cancellationToken);

        _ = await Assert.That(ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(error.Status.Detail)).IsTrue();
        _ = await Assert.That(frames).Count().IsEqualTo(2);
    }

    private static List<PersistedIdempotencyRecord> Export(FusedWriteHarness harness)
    {
        var exported = new List<PersistedIdempotencyRecord>();
        IIdempotencySnapshotExporter exporter = harness.Store;
        exporter.ExportSnapshot(exported, harness.Clock.GetUtcNow().UtcDateTime);
        return exported;
    }

    private static Task<SetAsyncResponse> RunAsync(FusedWriteHarness harness, string operationId, Func<Task> body, CancellationToken cancellationToken) =>
        harness.Coordinator.ExecuteAsync(
            operationId,
            Fingerprint,
            body,
            static async (run, _) =>
            {
                await run();
                return new SetAsyncResponse();
            },
            cancellationToken);

    private static Task SetAsync(FusedWriteHarness harness, string operationId, string value)
    {
        RpcMutationIdempotencyExecutionAmbient.RegisterOutcomeProjection<bool>(static _ => new SetAsyncResponse());
        return harness.Cache.SetEntryAsync(operationId, FusedWriteHarness.CacheName, FusedWriteHarness.Key, new NodeCacheEntry<object?>(value), CancellationToken.None).AsTask();
    }

    /// <summary>An append subscriber that fails once armed.</summary>
    [Immutable]
    private sealed class FailingSubscriber
    {
        private readonly StrongBox<bool> _armed;

        internal FailingSubscriber(StrongBox<bool> armed)
        {
            _armed = armed;
        }

        /// <summary>Fails the append it is told about once armed.</summary>
        /// <param name="sender">The journal.</param>
        /// <param name="args">The event arguments.</param>
        /// <exception cref="InvalidOperationException">The subscriber is armed.</exception>
        internal void OnAppended(object? sender, EventArgs args)
        {
            if (Volatile.Read(ref _armed.Value))
                throw new InvalidOperationException("append subscriber failed");
        }
    }
}
