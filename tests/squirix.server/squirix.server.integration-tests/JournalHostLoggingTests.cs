using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// The journal is built while the host is being composed, before the host logger exists. Its diagnostics must still reach the logger the host
/// registers, not a null logger (issue 715). Not run in parallel: the server logging bridge is process-wide, so a concurrently starting host
/// would take over the journal's diagnostics.
/// </summary>
[NotInParallel]
public sealed class JournalHostLoggingTests : NodeIntegrationTestBase
{
    private const int JournalWaitCanceledWhileStalledEventId = 1014;

    /// <summary>A stall warning logged by the host journal reaches the host logger under the journal coordinator category.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The host journal does not expose its stall probe.</exception>
    [Test]
    public async Task JournalEventsReachHostLogger(CancellationToken cancellationToken)
    {
        using var recorder = new CategoryRecordingLoggerProvider();
        var options = new IntegrationStartOptions { PersistenceOptions = new PersistenceOptions(), ServicesConfigure = recorder.Register };
        await using var cluster = await StartClusterAsync("node_journal_logging", options, cancellationToken);
        var node = cluster["node_journal_logging"];
        if (node.GetRequiredService<JournalCoordinatorHost>().Coordinator is not IJournalCoordinatorState journal)
            throw new InvalidOperationException("the host journal does not expose its stall probe.");

        // The node is idle, so its journal thread performs no segment I/O and the test is the only writer of the probe.
        var started = Stopwatch.GetTimestamp();
        journal.StallProbe.IoStarted(nameof(IJournalSegmentWriter.FlushToDisk));
        try
        {
            // The warning fires only once the I/O has been in progress for the slow-operation threshold.
            while (Stopwatch.GetElapsedTime(started).TotalMilliseconds <= JournalSlowOperationDiagnostics.WarningThresholdMs)
                await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, cancellationToken);

            journal.StallProbe.ReportWaitCanceled("durability commit");
        }
        finally
        {
            journal.StallProbe.IoFinished();
        }

        _ = await Assert.That(recorder.FindCategory(JournalWaitCanceledWhileStalledEventId)).IsEqualTo(typeof(JournalCoordinator).FullName);
    }

    /// <summary>Logger provider recording the event id and category of every entry the host logs.</summary>
    [ThreadSafe]
    private sealed class CategoryRecordingLoggerProvider : ILoggerProvider
    {
        private readonly ConcurrentQueue<(int EventId, string Category)> _events = new();

        public ILogger CreateLogger(string categoryName) => new CategoryLogger(this, categoryName);

        public void Dispose()
        {
        }

        /// <summary>Finds the category of the first entry with <paramref name="eventId" />.</summary>
        /// <param name="eventId">Event id to look for.</param>
        /// <returns>The category, or <see langword="null" /> when the host logger never received the event.</returns>
        internal string? FindCategory(int eventId)
        {
            foreach (var recorded in _events)
            {
                if (recorded.EventId == eventId)
                    return recorded.Category;
            }

            return null;
        }

        /// <summary>Registers this provider with the host logger factory.</summary>
        /// <param name="services">The host service collection.</param>
        internal void Register(IServiceCollection services) => _ = services.AddSingleton<ILoggerProvider>(this);

        private void Record(int eventId, string category) => _events.Enqueue((eventId, category));

        [Immutable]
        private sealed class CategoryLogger : ILogger
        {
            private readonly string _category;
            private readonly CategoryRecordingLoggerProvider _owner;

            internal CategoryLogger(CategoryRecordingLoggerProvider owner, string category)
            {
                _owner = owner;
                _category = category;
            }

            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
                _owner.Record(eventId.Id, _category);
        }
    }
}
