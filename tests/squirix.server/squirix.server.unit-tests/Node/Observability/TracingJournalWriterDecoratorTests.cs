using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Verifies <see cref="TracingJournalWriterDecorator" /> passes expected trace context to <see cref="IJournalOperationTracer" />.
/// </summary>
public sealed class TracingJournalWriterDecoratorTests : ServerUnitTestBase
{
    /// <summary>
    /// Append put through the decorator begins a journal put trace scope.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AppendPutAsyncCreatesJournalPutSpan()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-tracing-journal-decorator");
        try
        {
            var options = new PersistenceOptions { DataDir = dir, StrictFsync = true, JournalMaxSegmentMb = 16, FlushIntervalMs = 600_000 };
            var manifestStore = new ManifestStore(options);
            await using var core = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            var tracer = new RecordingJournalOperationTracer();
            await using var journal = new TracingJournalWriterDecorator(core, tracer);

            var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null);
            await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

            var (_, context) = Assert.Single(tracer.BeginCalls, static call => call.Kind == JournalOperationKind.Put);
            Assert.Equal("trace-key", context.Key);
            Assert.Equal(payload.Length, Assert.Single(tracer.FramePayloadBytes));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures traced journal puts reflect strict fsync and group-commit settings from persistence options.
    /// </summary>
    /// <param name="strictFsync">Whether strict fsync is enabled.</param>
    /// <param name="groupCommitMaxWaitMs">Group-commit wait window; zero disables group commit.</param>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Theory]
    [InlineData(true, 5)]
    [InlineData(false, 0)]
    public async Task AppendPutAsyncPutContextReflectsDurabilitySettings(bool strictFsync, int groupCommitMaxWaitMs)
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-tracing-journal-durability");
        try
        {
            var options = new PersistenceOptions
            {
                DataDir = dir,
                StrictFsync = strictFsync,
                JournalGroupCommitMaxWaitMs = groupCommitMaxWaitMs,
                JournalMaxSegmentMb = 16,
                FlushIntervalMs = 600_000,
            };
            var manifestStore = new ManifestStore(options);
            await using var core = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            var tracer = new RecordingJournalOperationTracer();
            await using var journal = new TracingJournalWriterDecorator(core, tracer);

            var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null);
            await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
            if (groupCommitMaxWaitMs > 0)
                await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

            var (_, context) = Assert.Single(tracer.BeginCalls, static call => call.Kind == JournalOperationKind.Put);
            Assert.Equal(strictFsync, context.StrictFsync);
            Assert.Equal(groupCommitMaxWaitMs > 0, context.GroupCommitEnabled);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
