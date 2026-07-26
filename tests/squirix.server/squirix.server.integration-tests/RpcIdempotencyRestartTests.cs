using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>Integration coverage for mutating gRPC idempotency across unclean node restarts.</summary>
public sealed class RpcIdempotencyRestartTests : NodeIntegrationTestBase
{
    private const string CompactScope = "idempotency-force-kill-compact";
    private const string Scope = "idempotency-force-kill";
    private const string SetScope = "idempotency-force-kill-set";
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>After compaction and SIGKILL-style restart a retry with the same operation id must replay Added=true.</summary>
    [Fact]
    public async Task ForceKillCompactionReplayTryAddOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-compact-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-c", new NodeStartOptions { UsePersistence = true, ExtraScope = CompactScope });
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
        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-c", new NodeStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = CompactScope });
        using (var channel = CreateGrpcChannel(restarted.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(retry.Added);
        }
    }

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay the original Set response.</summary>
    [Fact]
    public async Task ForceKillRestartReplaySetEntryOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new SetEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-set-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "set-value", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-b", new NodeStartOptions { UsePersistence = true, ExtraScope = SetScope });
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.NotNull(first);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-b", new NodeStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = SetScope });
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

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay Added=true even though the key was recovered from the journal.</summary>
    [Fact]
    public async Task ForceKillRestartReplayTryAddOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-a", new NodeStartOptions { UsePersistence = true, ExtraScope = Scope });
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(first.Added);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, DefaultCancellationToken);
        await AssertJournalContainsPutAndIdempotencyOutcomeAsync(node.DataDir);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-a", new NodeStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = Scope });
        using (var channel = CreateGrpcChannel(restarted.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(retry.Added);

            var get = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "force-kill-idempotency" }, cancellationToken: DefaultCancellationToken);
            Assert.True(get.Found);
        }
    }

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay the original Set response.</summary>
    [Fact]
    public async Task ForceKillRestartReplaySetEntryOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new SetEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-set-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "set-value", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-b", new NodeStartOptions { UsePersistence = true, ExtraScope = SetScope });
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var first = await client.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.NotNull(first);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-b", new NodeStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = SetScope });
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

    /// <summary>After SIGKILL-style restart a retry with the same operation id must replay Added=true even though the key was recovered from the journal.</summary>
    [Fact]
    public async Task ForceKillRestartReplayTryAddOperationIdResponse()
    {
        var uri = GetNextHttpUri();
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "force-kill-compact-idempotency",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var node = await StartNodeAsync(uri, "node-a", new NodeStartOptions { UsePersistence = true, ExtraScope = Scope });
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
        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var restartUri = GetNextHttpUri();
        await using var restarted = await StartNodeAsync(restartUri, "node-a", new NodeStartOptions { UsePersistence = true, CleanTestDir = false, ExtraScope = Scope });
        using (var channel = CreateGrpcChannel(restarted.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var retry = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
            Assert.True(retry.Added);

            var get = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "force-kill-idempotency" }, cancellationToken: DefaultCancellationToken);
            Assert.True(get.Found);
        }
    }

    private static async Task AssertJournalContainsPutAndIdempotencyOutcomeAsync(string dataDir)
    {
        var persistence = new PersistenceOptions { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
        using var manifestStore = new ManifestStore(persistence);
        var manifest = await manifestStore.ReadCurrentOrDefaultAsync(CancellationToken.None).ConfigureAwait(false);
        var sawPut = false;
        var sawIdempotency = false;
        using var records = JournalReadPath.ReadAll(dataDir, manifest.CurrentJournal, CancellationToken.None);
        while (records.MoveNext())
        {
            var record = records.Current;
            if (record.Operation is JournalOperationKind.Put)
                sawPut = true;
            if (record.Operation is JournalOperationKind.IdempotencyOutcome)
                sawIdempotency = true;
        }

        Assert.True(sawPut);
        Assert.True(sawIdempotency);
    }
}
