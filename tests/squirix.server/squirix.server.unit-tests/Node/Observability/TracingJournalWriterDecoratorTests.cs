using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Observability;

/// <summary>
/// Verifies <see cref="TracingJournalWriterDecorator" /> emits expected journal spans.
/// </summary>
public sealed class TracingJournalWriterDecoratorTests : ServerUnitTestBase
{
    /// <summary>
    /// Append put through the decorator creates a journal span.
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
            await using var journal = new TracingJournalWriterDecorator(core, new OpenTelemetryJournalOperationTracer());

            var started = new List<string>();
            using (CreateSquirixActivityListener(started))
            {
                var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null);
                await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
                await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
            }

            Assert.Contains("journal.put", started);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures traced journal puts include strict fsync and group-commit settings from persistence options.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AppendPutAsyncSpanIncludesDurabilityTagsFromPersistenceOptions()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-tracing-journal-durability-tags");
        try
        {
            var options = new PersistenceOptions
            {
                DataDir = dir,
                StrictFsync = true,
                JournalGroupCommitMaxWaitMs = 5,
                JournalMaxSegmentMb = 16,
                FlushIntervalMs = 600_000,
            };
            var manifestStore = new ManifestStore(options);
            await using var core = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            await using var journal = new TracingJournalWriterDecorator(core, new OpenTelemetryJournalOperationTracer());

            Activity? putActivity = null;
            using (CreateSquirixActivityListener(
                       null,
                       activity =>
                       {
                           if (activity.DisplayName == "journal.put")
                               putActivity = activity;
                       }))
            {
                var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null);
                await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
                await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);
            }

            Assert.NotNull(putActivity);
            Assert.Equal(true, putActivity.GetTagItem("journal.strict_fsync"));
            Assert.Equal(true, putActivity.GetTagItem("journal.group_commit"));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures traced journal puts reflect disabled strict fsync and group commit.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous test.</returns>
    [Fact]
    public async Task AppendPutAsyncSpanReflectsDisabledDurabilitySettings()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-tracing-journal-durability-off");
        try
        {
            var options = new PersistenceOptions
            {
                DataDir = dir,
                StrictFsync = false,
                JournalGroupCommitMaxWaitMs = 0,
                JournalMaxSegmentMb = 16,
                FlushIntervalMs = 600_000,
            };
            var manifestStore = new ManifestStore(options);
            await using var core = new JournalWriter(options, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            await using var journal = new TracingJournalWriterDecorator(core, new OpenTelemetryJournalOperationTracer());

            Activity? putActivity = null;
            using (CreateSquirixActivityListener(
                       null,
                       activity =>
                       {
                           if (activity.DisplayName == "journal.put")
                               putActivity = activity;
                       }))
            {
                var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null);
                await journal.AppendPutAsync(CacheKey.Default("trace-key"), payload, null, DefaultCancellationToken);
            }

            Assert.NotNull(putActivity);
            Assert.Equal(false, putActivity.GetTagItem("journal.strict_fsync"));
            Assert.Equal(false, putActivity.GetTagItem("journal.group_commit"));
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    private static ActivityListener CreateSquirixActivityListener(List<string> started) => CreateSquirixActivityListener(started, null);

    private static ActivityListener CreateSquirixActivityListener(List<string>? started, Action<Activity>? onStarted)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = static source => source.Name == "Squirix",
            Sample = static (ref _) => ActivitySamplingResult.AllData,
            ActivityStarted = activity =>
            {
                onStarted?.Invoke(activity);
                if (started is not null && !string.IsNullOrEmpty(activity.DisplayName))
                    started.Add(activity.DisplayName);
            },
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }
}
