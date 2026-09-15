using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader-owned expiration ordering and identity tests.</summary>
[Immutable]
public sealed class ReplicatedExpirationTests : ServerUnitTestBase
{
    /// <summary>Disposal stops admission and waits until an active key-gate lease is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposalDrainsActiveKeyLease(CancellationToken cancellationToken)
    {
        var pipeline = new ExpirationPipeline(true);
        await using var commit = CreateCommit(pipeline);
        var expiration = new ReplicaExpirationCoordinator(commit, true, 1);
        var touchEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseTouch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var touch = expiration.SerializeTouchAsync(
            "default",
            "key-a",
            async touchToken =>
            {
                touchEntered.SetResult();
                await releaseTouch.Task.WaitAsync(touchToken);
                return true;
            },
            cancellationToken);

        await touchEntered.Task.WaitAsync(cancellationToken);
        var disposal = expiration.DisposeAsync().AsTask();
        _ = await Assert.That(disposal.IsCompleted).IsFalse();

        var rejected = expiration.SerializeTouchAsync("default", "key-b", static _ => ValueTask.FromResult(true), cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, bool>(rejected);

        releaseTouch.SetResult();
        _ = await Assert.That(await touch).IsTrue();
        await disposal;
    }

    /// <summary>A post-append expiration failure uses the common stable ambiguity result.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpirationAfterAppendIsCommitUnknown(CancellationToken cancellationToken)
    {
        var pipeline = new ExpirationPipeline(false);
        var commit = CreateCommit(pipeline);
        try
        {
            await using var expiration = new ReplicaExpirationCoordinator(commit, true, 1);
            var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);

            var operation = expiration.CommitExpiredMissAsync(
                new ReplicaExpirationRequest
                {
                    GroupId = "group-a",
                    CacheName = "default",
                    Key = "key-a",
                    UtcNow = expiresUtc.AddTicks(1),
                    ReadRaw = _ => ValueTask.FromResult<ReplicaExpirationCandidate?>(new ReplicaExpirationCandidate(7, expiresUtc)),
                    PrepareTombstone = static (_, operationId) => CreateMutation(operationId),
                    Timeout = TimeSpan.FromSeconds(2),
                    CancellationToken = cancellationToken,
                });
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, bool>(operation);

            _ = await Assert.That(error.Message).Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, StringComparison.Ordinal);
        }
        finally
        {
            await commit.DisposeAsync();
        }
    }

    /// <summary>Operation ids are stable, domain-separated, lowercase 32-hex values.</summary>
    [Test]
    public async Task ExpirationIdIsStableAndSeparated()
    {
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);
        var first = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 7, expiresUtc);
        var repeated = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 7, expiresUtc);
        var boundary = ReplicaExpirationOperationId.Create("group-a", "defaul", "tkey-a", 7, expiresUtc);

        _ = await Assert.That(repeated).IsEqualTo(first);
        _ = await Assert.That(first).IsEqualTo("f6e3fa560b869c4cfa8a26062a016ee9");
        _ = await Assert.That(boundary).IsNotEqualTo(first, StringComparer.Ordinal);
        _ = await Assert.That(first.Length).IsEqualTo(32);
        _ = await Assert.That(first).Matches("^[0-9a-f]{32}$");
        _ = await Assert.That(ReplicaExpirationOperationId.OperationScope).IsEqualTo("replicated-expiration");
    }

    /// <summary>An expired read becomes a miss only after the tombstone is durably applied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredReadCommitsTombstoneBeforeMiss(CancellationToken cancellationToken)
    {
        var pipeline = new ExpirationPipeline(true);
        await using var commit = CreateCommit(pipeline);
        await using var expiration = new ReplicaExpirationCoordinator(commit, true, 2);
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);

        var missed = await expiration.CommitExpiredMissAsync(
            new ReplicaExpirationRequest
            {
                GroupId = "group-a",
                CacheName = "default",
                Key = "key-a",
                UtcNow = expiresUtc.AddTicks(1),
                ReadRaw = _ => ValueTask.FromResult<ReplicaExpirationCandidate?>(new ReplicaExpirationCandidate(7, expiresUtc)),
                PrepareTombstone = static (_, operationId) => CreateMutation(operationId),
                Timeout = TimeSpan.FromSeconds(2),
                CancellationToken = cancellationToken,
            });

        pipeline.Trace.Add("miss");
        _ = await Assert.That(missed).IsTrue();
        await SequenceAssert.Equal(["local", "follower", "follower", "commit", "apply", "miss"], pipeline.Trace, StringComparer.Ordinal);
        _ = await Assert.That(pipeline.Mutation!.OperationScope).IsEqualTo(ReplicaExpirationOperationId.OperationScope);
    }

    /// <summary>Follower mode never evaluates or deletes an expired entry independently.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FollowerDoesNotExpireIndependently(CancellationToken cancellationToken)
    {
        var pipeline = new ExpirationPipeline(true);
        await using var commit = CreateCommit(pipeline);
        await using var expiration = new ReplicaExpirationCoordinator(commit, false, 1);
        var readCount = 0;

        var missed = await expiration.CommitExpiredMissAsync(
            new ReplicaExpirationRequest
            {
                GroupId = "group-a",
                CacheName = "default",
                Key = "key-a",
                UtcNow = DateTime.UtcNow,
                ReadRaw = _ =>
                {
                    readCount++;
                    return ValueTask.FromResult<ReplicaExpirationCandidate?>(null);
                },
                PrepareTombstone = static (_, _) => throw new InvalidOperationException("Follower must not prepare expiration."),
                Timeout = TimeSpan.FromSeconds(2),
                CancellationToken = cancellationToken,
            });

        _ = await Assert.That(missed).IsFalse();
        _ = await Assert.That(readCount).IsEqualTo(0);
        _ = await Assert.That(pipeline.Trace).IsEmpty();
    }

    /// <summary>A raw read observes expiry without triggering local deletion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RawReadDoesNotDeleteExpiredEntry(CancellationToken cancellationToken)
    {
        var time = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var cache = new PhysicalCache<string>(time);
        var key = new CacheKey("default", "key-a");
        await cache.SetAsync(key, new NodeCacheEntry<string>("value", expiresUtc: DateTime.UnixEpoch.AddSeconds(1)), cancellationToken);
        time.Advance(TimeSpan.FromSeconds(2));

        var raw = await cache.RawReader.GetEntryRawAsync(key, cancellationToken);
        _ = await Assert.That(raw).IsNotNull();
        _ = await Assert.That(await cache.GetEntryAsync(key, cancellationToken)).IsNull();
        _ = await Assert.That(await cache.RawReader.GetEntryRawAsync(key, cancellationToken)).IsNull();
    }

    /// <summary>Expiration requests require identifiers, a UTC timestamp, and a positive timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsInvalidRequestContract(CancellationToken cancellationToken)
    {
        var pipeline = new ExpirationPipeline(false);
        await using var commit = CreateCommit(pipeline);
        await using var expiration = new ReplicaExpirationCoordinator(commit, true, 1);
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);

        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(
            expiration.CommitExpiredMissAsync(CreateRequest(new DateTime(expiresUtc.Ticks, DateTimeKind.Local), TimeSpan.FromSeconds(2))));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException, bool>(expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.Zero)));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.FromSeconds(2), string.Empty)));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(
            expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.FromSeconds(2), cacheName: string.Empty)));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.FromSeconds(2), key: string.Empty)));
        return;

        ReplicaExpirationRequest CreateRequest(DateTime utcNow, TimeSpan timeout, string groupId = "group-a", string cacheName = "default", string key = "key-a")
        {
            return new ReplicaExpirationRequest
            {
                GroupId = groupId,
                CacheName = cacheName,
                Key = key,
                UtcNow = utcNow,
                ReadRaw = static _ => ValueTask.FromResult<ReplicaExpirationCandidate?>(null),
                PrepareTombstone = static (_, _) => throw new InvalidOperationException("Unreachable."),
                Timeout = timeout,
                CancellationToken = cancellationToken,
            };
        }
    }

    /// <summary>Touch and expiration callbacks for one key execute in a single observable order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchAndExpirationShareKeyGate(CancellationToken cancellationToken)
    {
        var pipeline = new ExpirationPipeline(true);
        await using var commit = CreateCommit(pipeline);
        await using var expiration = new ReplicaExpirationCoordinator(commit, true, 2);
        var touchEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseTouch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var expirationRead = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var touch = expiration.SerializeTouchAsync(
            "default",
            "key-a",
            async touchToken =>
            {
                touchEntered.SetResult();
                await releaseTouch.Task.WaitAsync(touchToken);
                return true;
            },
            cancellationToken);
        await touchEntered.Task.WaitAsync(cancellationToken);

        var miss = expiration.CommitExpiredMissAsync(
            new ReplicaExpirationRequest
            {
                GroupId = "group-a",
                CacheName = "default",
                Key = "key-a",
                UtcNow = DateTime.UtcNow,
                ReadRaw = _ =>
                {
                    expirationRead.SetResult();
                    return ValueTask.FromResult<ReplicaExpirationCandidate?>(null);
                },
                PrepareTombstone = static (_, _) => throw new InvalidOperationException("No tombstone expected."),
                Timeout = TimeSpan.FromSeconds(2),
                CancellationToken = cancellationToken,
            });

        _ = await Assert.That(expirationRead.Task.IsCompleted).IsFalse();
        releaseTouch.SetResult();
        _ = await Assert.That(await touch).IsTrue();
        _ = await Assert.That(await miss).IsFalse();
        _ = await Assert.That(expirationRead.Task.IsCompleted).IsTrue();
    }

    private static ReplicaCommitCoordinator CreateCommit(ExpirationPipeline pipeline)
    {
        var hooksExpectations = new IReplicaCommitFaultHooksCreateExpectations();
        _ = hooksExpectations.Setups.OnStageAsync(Arg.Any<ReplicaCommitStage>(), Arg.Any<PreparedReplicaMutation>(), Arg.Any<CancellationToken>())
                             .ReturnValue(ValueTask.CompletedTask);
        return new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(3, 0, 0, 2),
            pipeline,
            hooksExpectations.Instance(),
            new GroupIdempotencyState(8, TimeSpan.MaxValue));
    }

    private static PreparedReplicaMutation CreateMutation(string operationId) => new(
        new ReplicaOperationIdentity("group-a", ReplicaExpirationOperationId.OperationScope, operationId, new byte[] { 1 }),
        1,
        1,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 7));

    [Mutable]
    private sealed class ExpirationPipeline : IReplicaCommitPipeline
    {
        private readonly bool _acknowledge;

        internal ExpirationPipeline(bool acknowledge)
        {
            _acknowledge = acknowledge;
        }

        internal PreparedReplicaMutation? Mutation { get; private set; }

        internal List<string> Trace { get; } = [];

        public ValueTask AdvanceCommitIndexAsync(ulong commitIndex, CancellationToken cancellationToken)
        {
            Trace.Add("commit");
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            if (!_acknowledge)
                return ValueTask.FromException<ReplicaDurableAcknowledgement>(new TimeoutException());

            Trace.Add("follower");
            var result = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
            return ValueTask.FromResult(result);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Mutation = mutation;
            Trace.Add("local");
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            Trace.Add("apply");
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
        }
    }
}
