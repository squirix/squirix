using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Serialization.Protobuf;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Core.Enums;

namespace Squirix.Server.IntegrationTests;

/// <summary>Journal group commit enabled through the public server options on a hosted node, driven over gRPC.</summary>
public sealed class GroupCommitHostingTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const int Concurrency = 64;
    private const int MaxBursts = 20;
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(60);

    /// <summary>The default public options keep the journal on per-mutation flushes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultOptionsKeepGroupCommitOff(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-commit-default");
        var (app, _) = await StartAsync(dir, null, cancellationToken);
        await using (app)
        {
            _ = await Assert.That(app.Services.GetRequiredService<PersistenceOptions>().IsJournalGroupCommitEnabled).IsFalse();
            _ = await Assert.That(GetJournal(app).IsJournalGroupCommitEnabled).IsFalse();
        }
    }

    /// <summary>A lone write waits out the group commit window, so the journal really holds writes back for grouping.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LoneWriteWaitsForTheWindow(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-commit-lone");
        var (app, uri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (app)
        {
            using var channel = CreateBinaryChannel(uri);
            var client = new CacheWireClient(channel);
            _ = await client.SetEntryAsync(CreateSet("warm-up", "warm"), cancellationToken: cancellationToken);

            var started = TimeProvider.System.GetTimestamp();
            _ = await client.SetEntryAsync(CreateSet("lone", "value"), cancellationToken: cancellationToken);
            var elapsed = TimeProvider.System.GetElapsedTime(started);

            // The batch holds one write and is not full, so it flushes only once the 100 ms window ends; the bound leaves slack for clock rounding.
            _ = await Assert.That(elapsed).IsGreaterThanOrEqualTo(TimeSpan.FromMilliseconds(90));
        }
    }

    /// <summary>Concurrent writes on distinct keys over gRPC share journal flushes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DistinctKeyWritesShareFlushes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-commit-distinct");
        var (app, uri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (app)
        {
            _ = await Assert.That(GetJournal(app).IsJournalGroupCommitEnabled).IsTrue();

            using var channel = CreateBinaryChannel(uri);
            var client = new CacheWireClient(channel);

            // The first call opens the connection and the segment, so the measured writes arrive together.
            _ = await client.SetEntryAsync(CreateSet("warm-up", "warm"), cancellationToken: cancellationToken);
            var flushesBefore = GetJournal(app).FlushCount;

            // The public window is capped at 100 ms, so a starved burst can spread its writes past it; bursts repeat until one batch holds two writes.
            var written = 0;
            for (var round = 0; round < MaxBursts && (GetJournal(app).GroupCommit?.LargestBatch ?? 0) < 2; round++)
            {
                await WriteBurstAsync(client, round, cancellationToken);
                written += Concurrency;
            }

            var flushes = GetJournal(app).FlushCount - flushesBefore;
            _ = await Assert.That(flushes).IsGreaterThan(0L);
            _ = await Assert.That(flushes).IsLessThan(written);

            // Ungrouped checkpoints coalesce as well, so only the group commit batch size proves that writes were held and flushed together.
            _ = await Assert.That(GetJournal(app).GroupCommit?.LargestBatch ?? 0).IsGreaterThanOrEqualTo(2);
        }
    }

    /// <summary>Concurrent mutations of one key all complete, and a restart on the same directory recovers the state they left.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [ExecutionPriority(Priority.High)]
    public async Task SameKeyMutationsSurviveRestart(CancellationToken cancellationToken)
    {
        const string key = "same-key";
        using var dir = new TempDirectory("squirix-group-commit-same-key");
        GetValueAsyncResponse before;
        var (app, uri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (app)
        {
            using var channel = CreateBinaryChannel(uri);
            var client = new CacheWireClient(channel);

            var mutations = new Task[Concurrency];
            for (var i = 0; i < Concurrency; i++)
                mutations[i] = MutateAsync(client, key, i, cancellationToken);

            // An Internal or AlreadyExists status from any call surfaces here as an RpcException.
            await Task.WhenAll(mutations).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            before = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        }

        var (restarted, restartedUri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (restarted)
        {
            using var channel = CreateBinaryChannel(restartedUri);
            var client = new CacheWireClient(channel);

            var after = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);

            // Whatever state the burst left, Set, Remove or Touch last, the restart replays the journal to exactly that state.
            _ = await Assert.That(after.Found).IsEqualTo(before.Found);
            _ = await Assert.That(after.Value).IsEqualTo(before.Value);
        }
    }

    private static void ConfigureGroupCommit(SquirixServerOptions options)
    {
        options.Journal.GroupCommitMaxWait = TimeSpan.FromMilliseconds(100);
        options.Journal.GroupCommitMaxBatch = Concurrency;
    }

    private static async Task WriteBurstAsync(CacheWireClient client, int round, CancellationToken cancellationToken)
    {
        var writes = new Task[Concurrency];
        for (var i = 0; i < Concurrency; i++)
            writes[i] = client.SetEntryAsync(CreateSet($"distinct-{round}-{i}", "value"), cancellationToken: cancellationToken);

        await Task.WhenAll(writes).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
    }

    private static SetEntryAsyncRequest CreateSet(string key, string value) => new()
    {
        OperationId = Guid.NewGuid().ToString("N"),
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
    };

    private static JournalCoordinator GetJournal(ServerApplication app) =>
        ThrowHelper.Required(app.Services.GetRequiredService<JournalCoordinatorHost>().Coordinator as JournalCoordinator, "The hosted journal is not a JournalCoordinator.");

    private static Task MutateAsync(CacheWireClient client, string key, int index, CancellationToken cancellationToken)
    {
        var operationId = Guid.NewGuid().ToString("N");
        return (index % 3) switch
        {
            0 => client.SetEntryAsync(CreateSet(key, $"value-{index}"), cancellationToken: cancellationToken),
            1 => client.RemoveAsync(new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken),
            _ => client.TouchAsync(
                new TouchAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId, Expiration = Duration.FromTimeSpan(TimeSpan.FromHours(1)) },
                cancellationToken: cancellationToken),
        };
    }

    private static async Task<(ServerApplication App, Uri Uri)> StartAsync(string dataDirectory, Action<SquirixServerOptions>? configure, CancellationToken cancellationToken)
    {
        var services = TestNodeBuilder.CreateServices();
        var uri = GetNextHttpUri();
        ListenPortPool.ReleaseHeldPrimary(uri);
        _ = await services.ConfigureNodeAsync(
            options =>
            {
                options.Uri = uri;
                options.UsePersistence(dataDirectory);
                configure?.Invoke(options);
            },
            loadDiscoveredSettings: false,
            configureComposition: static args => args.ManagementUri = new Uri("https://127.0.0.1:0"),
            cancellationToken: cancellationToken);

        var app = services.BuildNode();
        await SquirixServer.ApplicationHandle.StartWithRollbackAsync(app, static (node, token) => ServerHostingComposition.OpenStorageAsync(node.Services, token), cancellationToken);
        return (app, uri);
    }
}
