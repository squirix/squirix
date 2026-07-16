using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Verifies <see cref="TracingJournalCoordinatorDecorator" /> passes expected trace context to <see cref="IJournalOperationTracer" />.
/// </summary>
public sealed class TracingJournalCoordinatorDecoratorTests : UnitTestBase
{
    /// <summary>Append put through the decorator begins a journal put trace scope.</summary>
    [Fact]
    public async Task AppendPutAsyncCreatesJournalPutSpan()
    {
        using var dir = new TempDirectory("squirix-tracing-journal-decorator");
        var options = new PersistenceOptions { DataDir = dir, JournalMaxSegmentMb = 16, FlushIntervalMs = 600_000 };
        using var manifestStore = new ManifestStore(options);
        await using var core = await JournalCoordinatorFactory.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var tracer = new RecordingJournalOperationTracer();
        await using var journal = new TracingJournalCoordinatorDecorator(core, tracer);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var (_, context) = Assert.Single(tracer.BeginCalls, static call => call.Kind is JournalOperationKind.Put);
        Assert.Equal("trace-key", context.Key);
        Assert.Equal(payload.Length, context.PayloadBytes);
    }

    /// <summary>Ensures traced journal puts reflect strict fsync and group-commit settings from persistence options.</summary>
    /// <param name="groupCommitMaxWaitMilliseconds">Group-commit wait window; zero disables group commit.</param>
    [Theory]
    [InlineData(5)]
    [InlineData(0)]
    public async Task AppendPutAsyncPutContextReflectsDurabilitySettings(int groupCommitMaxWaitMilliseconds)
    {
        using var dir = new TempDirectory("squirix-tracing-journal-durability");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(groupCommitMaxWaitMilliseconds),
            JournalMaxSegmentMb = 16,
            FlushIntervalMs = 600_000,
        };
        using var manifestStore = new ManifestStore(options);
        await using var core = await JournalCoordinatorFactory.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var tracer = new RecordingJournalOperationTracer();
        await using var journal = new TracingJournalCoordinatorDecorator(core, tracer);

        var payload = JournalEntryPayloadKit.EncodePut("v");
        await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, DefaultCancellationToken);
        if (groupCommitMaxWaitMilliseconds > 0)
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var (_, context) = Assert.Single(tracer.BeginCalls, static call => call.Kind is JournalOperationKind.Put);
        Assert.Equal(groupCommitMaxWaitMilliseconds > 0, context.GroupCommitEnabled);
    }
}
