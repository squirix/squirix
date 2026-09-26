using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Utils;

/// <summary>
/// Loggers handed out by <see cref="LogManager" /> write to the host logger factory configured when they log, so components built before the
/// host configures logging (the journal is built while the host is composed) are not left with a null logger (issue 715). Not run in parallel:
/// the configured factory is process-wide.
/// </summary>
[Immutable]
[NotInParallel]
public sealed class LogManagerHostTests
{
    private const int JournalThreadJoinTimedOutEventId = 3013;

    /// <summary>A logger taken before the host factory is configured logs to that factory once it is.</summary>
    [Test]
    public async Task EarlyLoggerReachesLaterHostFactory()
    {
        var logger = LogManager.GetLogger<LogManagerHostTests>();
        var log = new EventRecordingLogger();
        using var provider = new RecordingLoggerProvider(log);
        using var factory = new LoggerFactory([provider]);
        LogManager.Configure(factory);

        LogManager.JournalThreadJoinTimedOut(logger, 0);

        _ = await Assert.That(log.Find(JournalThreadJoinTimedOutEventId)?.Level).IsEqualTo(LogLevel.Error);
    }

    /// <summary>Once the configured host factory is disposed, a logger discards its entries instead of throwing.</summary>
    [Test]
    public async Task DisposedHostFactoryDiscardsEntries()
    {
        var logger = LogManager.GetLogger("Squirix.Server.UnitTests.Utils.LogManagerHostTests");
        var log = new EventRecordingLogger();
        using var provider = new RecordingLoggerProvider(log);
        var factory = new LoggerFactory([provider]);
        LogManager.Configure(factory);
        factory.Dispose();

        LogManager.JournalThreadJoinTimedOut(logger, 0);

        _ = await Assert.That(logger.IsEnabled(LogLevel.Error)).IsFalse();
        _ = await Assert.That(log.Count(JournalThreadJoinTimedOutEventId)).IsEqualTo(0);
    }

    /// <summary>Logger provider handing out one recording logger for every category.</summary>
    [Immutable]
    private sealed class RecordingLoggerProvider : ILoggerProvider
    {
        private readonly EventRecordingLogger _log;

        internal RecordingLoggerProvider(EventRecordingLogger log)
        {
            _log = log;
        }

        public ILogger CreateLogger(string categoryName) => _log;

        public void Dispose()
        {
        }
    }
}
