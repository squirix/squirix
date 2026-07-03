using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Reliability;

/// <summary>Integration coverage for mutating gRPC idempotency across unclean node restarts.</summary>
public sealed class RpcMutationIdempotencyRestartIntegrationTests : IntegrationTestBase
{
    private const string Scope = "idempotency-force-kill";
    private const string SetScope = "idempotency-force-kill-set";
    private const string CompactScope = "idempotency-force-kill-compact";
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay Added=true even though the key was recovered from the journal.</summary>
    [Fact]
    public async Task ForceKillRestartShouldReplayTryAddOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-idempotency",
            Entry = new CacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-a", usePersistence: true, extraScope: Scope);
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(first.Added);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-a", usePersistence: true, cleanTestDir: false, extraScope: Scope);
        using (var channel = CreateGrpcChannel(restarted.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(retry.Added);

            var get = await client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "force-kill-idempotency" },
                cancellationToken: DefaultCancellationToken);
            Assert.True(get.Found);
        }
    }

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay the original Set response.</summary>
    [Fact]
    public async Task ForceKillRestartShouldReplaySetEntryOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new SetEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-set-idempotency",
            Entry = new CacheEntry<object?> { Value = "set-value", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-b", usePersistence: true, extraScope: SetScope);
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.NotNull(first);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-b", usePersistence: true, cleanTestDir: false, extraScope: SetScope);
        using (var channel = CreateGrpcChannel(restarted.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.NotNull(retry);

            var get = await client.GetValueAsync(
                new GetValueAsyncRequest { CacheName = "default", Key = "force-kill-set-idempotency" },
                cancellationToken: DefaultCancellationToken);
            Assert.True(get.Found);
        }
    }

    /// <summary>After compaction and SIGKILL-style restart a retry with the same operation id must replay Added=true.</summary>
    [Fact]
    public async Task ForceKillAfterCompactionShouldReplayTryAddOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-compact-idempotency",
            Entry = new CacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-c", usePersistence: true, extraScope: CompactScope);
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(first.Added);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, DefaultCancellationToken);

        var persistence = new PersistenceOptions { DataDir = node.DataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        using var manifestStore = new ManifestStore(persistence);
        await JournalCompactor.CompactAsync(persistence, manifestStore, SnapshotStoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-c", usePersistence: true, cleanTestDir: false, extraScope: CompactScope);
        using (var channel = CreateGrpcChannel(restarted.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(retry.Added);
        }
    }
}
