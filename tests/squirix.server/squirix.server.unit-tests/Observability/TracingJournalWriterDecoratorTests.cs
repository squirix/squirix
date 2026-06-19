using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.PipelinedWal;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Verifies <see cref="TracingJournalWriterDecorator" /> passes expected trace context to <see cref="IJournalOperationTracer" />.
/// </summary>
public sealed class TracingJournalWriterDecoratorTests : UnitTestBase
{
    /// <summary>Append put through the decorator begins a journal put trace scope.</summary>
    [Fact]
    public async Task AppendPutAsyncCreatesJournalPutSpan()
    {
        using var dir = new TempDirectory("squirix-tracing-journal-decorator");
        var options = new PersistenceOptions { DataDir = dir, JournalBackend = JournalBackend.JsonFramed, JournalMaxSegmentMb = 16, FlushIntervalMs = 600_000 };
        using var manifestStore = new ManifestStore(options);
        await using var core = await JournalWriter.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var tracer = new RecordingJournalOperationTracer();
        await using var journal = new TracingJournalWriterDecorator(core, tracer);

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync("v", null, null, 1, null);
        await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
        await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var (_, context) = Assert.Single(tracer.BeginCalls, static call => call.Kind is JournalOperationKind.Put);
        Assert.Equal("trace-key", context.Key);
        Assert.Equal(payload.Length, Assert.Single(tracer.FramePayloadBytes));
    }

    /// <summary>Ensures traced journal puts reflect strict fsync and group-commit settings from persistence options.</summary>
    /// <param name="groupCommitMaxWaitMs">Group-commit wait window; zero disables group commit.</param>
    [Theory]
    [InlineData(5)]
    [InlineData(0)]
    public async Task AppendPutAsyncPutContextReflectsDurabilitySettings(int groupCommitMaxWaitMs)
    {
        using var dir = new TempDirectory("squirix-tracing-journal-durability");
        var options = new PersistenceOptions
        {
            DataDir = dir,
            JournalBackend = JournalBackend.JsonFramed,
            JournalGroupCommitMaxWaitMs = groupCommitMaxWaitMs,
            JournalMaxSegmentMb = 16,
            FlushIntervalMs = 600_000,
        };
        using var manifestStore = new ManifestStore(options);
        await using var core = await JournalWriter.CreateAsync(options, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var tracer = new RecordingJournalOperationTracer();
        await using var journal = new TracingJournalWriterDecorator(core, tracer);

        var payload = await DiscriminatedEntryJsonWriter.BuildEntryJsonAsync("v", null, null, 1, null);
        await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
        if (groupCommitMaxWaitMs > 0)
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

        var (_, context) = Assert.Single(tracer.BeginCalls, static call => call.Kind is JournalOperationKind.Put);
        Assert.Equal(groupCommitMaxWaitMs > 0, context.GroupCommitEnabled);
    }
}
