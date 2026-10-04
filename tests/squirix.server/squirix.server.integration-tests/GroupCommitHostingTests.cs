using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Journal group commit enabled through the public server options on a hosted node, driven over gRPC.</summary>
public sealed class GroupCommitHostingTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const int Concurrency = 64;
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
            using var channel = CreateGrpcChannel(uri);
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
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

            using var channel = CreateGrpcChannel(uri);
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

            // The first call opens the connection and the segment, so the measured writes arrive together.
            _ = await client.SetEntryAsync(CreateSet("warm-up", "warm"), cancellationToken: cancellationToken);
            var flushesBefore = GetJournal(app).FlushCount;

            var writes = new Task[Concurrency];
            for (var i = 0; i < Concurrency; i++)
                writes[i] = client.SetEntryAsync(CreateSet($"distinct-{i}", "value"), cancellationToken: cancellationToken).ResponseAsync;

            await Task.WhenAll(writes).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            var flushes = GetJournal(app).FlushCount - flushesBefore;
            _ = await Assert.That(flushes).IsGreaterThan(0L);
            _ = await Assert.That(flushes).IsLessThan(Concurrency);

            // Ungrouped checkpoints coalesce as well, so only the group commit batch size proves that writes were held and flushed together.
            _ = await Assert.That(GetJournal(app).GroupCommit?.LargestBatch ?? 0).IsGreaterThanOrEqualTo(2);
        }
    }

    /// <summary>Concurrent mutations of one key all complete, and a restart on the same directory recovers the state they left.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SameKeyMutationsSurviveRestart(CancellationToken cancellationToken)
    {
        const string key = "same-key";
        using var dir = new TempDirectory("squirix-group-commit-same-key");
        GetValueAsyncResponse before;
        var (app, uri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (app)
        {
            using var channel = CreateGrpcChannel(uri);
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

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
            using var channel = CreateGrpcChannel(restartedUri);
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

            var after = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);

            // Whatever state the burst left, Set, Remove or Touch last, the restart replays the journal to exactly that state.
            _ = await Assert.That(after.Found).IsEqualTo(before.Found);
            _ = await Assert.That(after.Value).IsEqualTo(before.Value);
        }
    }

    /// <summary>A write parked in an open group commit window when the host stops gracefully succeeds, and the restart recovers it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WriteInFlightAtStopSucceeds(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-group-commit-stop");
        var (app, uri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (app)
        {
            using var channel = CreateGrpcChannel(uri);
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            var write = client.SetEntryAsync(CreateSet("parked", "value"), cancellationToken: cancellationToken).ResponseAsync;

            // The frame is on the ring while its wait is still inside the open window, so the stop races the window end.
            var journal = GetJournal(app);
            while (journal.AppendedOps < 1)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await Task.Yield();
            }

            await app.StopAsync(cancellationToken);
            _ = await write.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }

        var (restarted, restartedUri) = await StartAsync(dir, ConfigureGroupCommit, cancellationToken);
        await using (restarted)
        {
            using var restartedChannel = CreateGrpcChannel(restartedUri);
            var restartedClient = new SquirixCacheService.SquirixCacheServiceClient(restartedChannel);
            var after = await restartedClient.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = "parked" }, cancellationToken: cancellationToken);
            _ = await Assert.That(after.Found).IsTrue();
        }
    }

    private static void ConfigureGroupCommit(SquirixServerOptions options)
    {
        options.Journal.GroupCommitMaxWait = TimeSpan.FromMilliseconds(100);
        options.Journal.GroupCommitMaxBatch = Concurrency;
    }

    private static SetEntryAsyncRequest CreateSet(string key, string value) => new()
    {
        OperationId = Guid.NewGuid().ToString("N"),
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToProto(),
    };

    private static JournalCoordinator GetJournal(WebApplication app) =>
        ThrowHelper.Required(app.Services.GetRequiredService<JournalCoordinatorHost>().Coordinator as JournalCoordinator, "The hosted journal is not a JournalCoordinator.");

    private static Task MutateAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, int index, CancellationToken cancellationToken)
    {
        var operationId = Guid.NewGuid().ToString("N");
        return (index % 3) switch
        {
            0 => client.SetEntryAsync(CreateSet(key, $"value-{index}"), cancellationToken: cancellationToken).ResponseAsync,
            1 => client.RemoveAsync(new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken).ResponseAsync,
            _ => client.TouchAsync(
                new TouchAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId, Expiration = Duration.FromTimeSpan(TimeSpan.FromHours(1)) },
                cancellationToken: cancellationToken).ResponseAsync,
        };
    }

    private static async Task<(WebApplication App, Uri Uri)> StartAsync(string dataDirectory, Action<SquirixServerOptions>? configure, CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [], ApplicationName = "Squirix.Server" });
        var uri = GetNextHttpUri();
        ListenPortPool.ReleaseHeldPrimary(uri);
        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.Uri = uri;
                options.UsePersistence(dataDirectory);
                configure?.Invoke(options);
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        var app = builder.Build();
        try
        {
            _ = await app.MapSquirixServerAsync(cancellationToken);
            await app.StartAsync(cancellationToken);
            return (app, uri);
        }
        catch
        {
            await app.DisposeAsync();
            throw;
        }
    }
}
