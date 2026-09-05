using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Leader-owned expiration ordering and identity tests.</summary>
[Immutable]
public sealed class ReplicatedExpirationTests : ServerUnitTestBase
{
    /// <summary>Disposal stops admission and waits until an active key-gate lease is released.</summary>
    [Fact]
    public async Task DisposalDrainsActiveKeyLease()
    {
        var pipeline = new ExpirationPipeline(true);
        await using var commit = CreateCommit(pipeline);
        var expiration = new ReplicaExpirationCoordinator(commit, true, 1);
        var touchEntered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var releaseTouch = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var touch = expiration.SerializeTouchAsync(
            "default",
            "key-a",
            async cancellationToken =>
            {
                touchEntered.SetResult();
                await releaseTouch.Task.WaitAsync(cancellationToken);
                return true;
            },
            DefaultCancellationToken);

        await touchEntered.Task.WaitAsync(DefaultCancellationToken);
        var disposal = expiration.DisposeAsync().AsTask();
        Assert.False(disposal.IsCompleted);

        var rejected = expiration.SerializeTouchAsync("default", "key-b", static _ => ValueTask.FromResult(true), DefaultCancellationToken);
        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException, bool>(rejected);

        releaseTouch.SetResult();
        Assert.True(await touch);
        await disposal;
    }

    /// <summary>A post-append expiration failure uses the common stable ambiguity result.</summary>
    [Fact]
    public async Task ExpirationAfterAppendIsCommitUnknown()
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
                    PrepareTombstone = static (candidate, operationId) => CreateMutation(candidate, operationId),
                    Timeout = TimeSpan.FromSeconds(2),
                    CancellationToken = DefaultCancellationToken,
                });
            var error = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, bool>(operation);

            Assert.Contains(ReplicaCommitCoordinator.CommitOutcomeUnknownCode, error.Message, StringComparison.Ordinal);
        }
        finally
        {
            await commit.DisposeAsync();
        }
    }

    /// <summary>Operation ids are stable, domain-separated, lowercase 32-hex values.</summary>
    [Fact]
    public void ExpirationIdIsStableAndSeparated()
    {
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);
        var first = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 7, expiresUtc);
        var repeated = ReplicaExpirationOperationId.Create("group-a", "default", "key-a", 7, expiresUtc);
        var boundary = ReplicaExpirationOperationId.Create("group-a", "defaul", "tkey-a", 7, expiresUtc);

        Assert.Equal(first, repeated);
        Assert.Equal("f6e3fa560b869c4cfa8a26062a016ee9", first);
        Assert.NotEqual(first, boundary, StringComparer.Ordinal);
        Assert.Equal(32, first.Length);
        Assert.Matches("^[0-9a-f]{32}$", first);
        Assert.Equal("replicated-expiration", ReplicaExpirationOperationId.OperationScope);
    }

    /// <summary>An expired read becomes a miss only after the tombstone is durably applied.</summary>
    [Fact]
    public async Task ExpiredReadCommitsTombstoneBeforeMiss()
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
                PrepareTombstone = static (candidate, operationId) => CreateMutation(candidate, operationId),
                Timeout = TimeSpan.FromSeconds(2),
                CancellationToken = DefaultCancellationToken,
            });

        pipeline.Trace.Add("miss");
        Assert.True(missed);
        Assert.Equal(["local", "follower", "follower", "commit", "apply", "miss"], pipeline.Trace);
        Assert.Equal(ReplicaExpirationOperationId.OperationScope, pipeline.Mutation!.OperationScope);
    }

    /// <summary>Follower mode never evaluates or deletes an expired entry independently.</summary>
    [Fact]
    public async Task FollowerDoesNotExpireIndependently()
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
                CancellationToken = DefaultCancellationToken,
            });

        Assert.False(missed);
        Assert.Equal(0, readCount);
        Assert.Empty(pipeline.Trace);
    }

    /// <summary>A raw read observes expiry without triggering local deletion.</summary>
    [Fact]
    public async Task RawReadDoesNotDeleteExpiredEntry()
    {
        var time = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var cache = new PhysicalCache<string>(time);
        var key = new CacheKey("default", "key-a");
        await cache.SetAsync(key, new NodeCacheEntry<string>("value", expiresUtc: DateTime.UnixEpoch.AddSeconds(1)), DefaultCancellationToken);
        time.Advance(TimeSpan.FromSeconds(2));

        var raw = await cache.RawReader.GetEntryRawAsync(key, DefaultCancellationToken);
        Assert.NotNull(raw);
        Assert.Null(await cache.GetEntryAsync(key, DefaultCancellationToken));
        Assert.Null(await cache.RawReader.GetEntryRawAsync(key, DefaultCancellationToken));
    }

    /// <summary>Expiration requests require identifiers, a UTC timestamp, and a positive timeout.</summary>
    [Fact]
    public async Task RejectsInvalidRequestContract()
    {
        var pipeline = new ExpirationPipeline(false);
        await using var commit = CreateCommit(pipeline);
        await using var expiration = new ReplicaExpirationCoordinator(commit, true, 1);
        var expiresUtc = new DateTime(638900000000000000, DateTimeKind.Utc);

        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(
            expiration.CommitExpiredMissAsync(CreateRequest(new DateTime(expiresUtc.Ticks, DateTimeKind.Local), TimeSpan.FromSeconds(2))));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException, bool>(expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.Zero)));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(
            expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.FromSeconds(2), string.Empty)));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(
            expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.FromSeconds(2), cacheName: string.Empty)));
        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentException, bool>(
            expiration.CommitExpiredMissAsync(CreateRequest(expiresUtc, TimeSpan.FromSeconds(2), key: string.Empty)));
        return;

        static ReplicaExpirationRequest CreateRequest(DateTime utcNow, TimeSpan timeout, string groupId = "group-a", string cacheName = "default", string key = "key-a")
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
                CancellationToken = DefaultCancellationToken,
            };
        }
    }

    /// <summary>Touch and expiration callbacks for one key execute in a single observable order.</summary>
    [Fact]
    public async Task TouchAndExpirationShareKeyGate()
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
            async cancellationToken =>
            {
                touchEntered.SetResult();
                await releaseTouch.Task.WaitAsync(cancellationToken);
                return true;
            },
            DefaultCancellationToken);
        await touchEntered.Task.WaitAsync(DefaultCancellationToken);

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
                CancellationToken = DefaultCancellationToken,
            });

        Assert.False(expirationRead.Task.IsCompleted);
        releaseTouch.SetResult();
        Assert.True(await touch);
        Assert.False(await miss);
        Assert.True(expirationRead.Task.IsCompleted);
    }

    private static ReplicaCommitCoordinator CreateCommit(ExpirationPipeline pipeline) => new(
        new ReplicaCommitCoordinatorOptions(3, 0, 0, 2),
        pipeline,
        NoOpHooks.Instance,
        new GroupIdempotencyState(8, TimeSpan.MaxValue));

    private static PreparedReplicaMutation CreateMutation(ReplicaExpirationCandidate candidate, string operationId) => new(
        new ReplicaOperationIdentity("group-a", ReplicaExpirationOperationId.OperationScope, operationId, new byte[] { 1 }),
        1,
        1,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 7),
        candidate.ExpiresUtc.Ticks);

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
            _ = commitIndex;
            _ = cancellationToken;
            Trace.Add("commit");
            return ValueTask.CompletedTask;
        }

        public ValueTask<ReplicaDurableAcknowledgement> AppendFollowerAsync(int replicaIndex, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = replicaIndex;
            _ = cancellationToken;
            if (!_acknowledge)
                return ValueTask.FromException<ReplicaDurableAcknowledgement>(new TimeoutException());

            Trace.Add("follower");
            var result = new ReplicaDurableAcknowledgement(mutation.GroupId, mutation.Term, mutation.LogIndex, mutation.OperationFingerprint, mutation.PayloadChecksum, true, true);
            return ValueTask.FromResult(result);
        }

        public ValueTask AppendLocalAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            Mutation = mutation;
            Trace.Add("local");
            return ValueTask.CompletedTask;
        }

        public ValueTask ApplyMemoryAsync(PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = mutation;
            _ = cancellationToken;
            Trace.Add("apply");
            return ValueTask.CompletedTask;
        }

        public void RecordLaggingReplica(int replicaIndex, ulong logIndex)
        {
            _ = replicaIndex;
            _ = logIndex;
        }
    }

    private sealed class NoOpHooks : IReplicaCommitFaultHooks
    {
        internal static NoOpHooks Instance { get; } = new();

        public ValueTask OnStageAsync(ReplicaCommitStage stage, PreparedReplicaMutation mutation, CancellationToken cancellationToken)
        {
            _ = stage;
            _ = mutation;
            _ = cancellationToken;
            return ValueTask.CompletedTask;
        }
    }
}
