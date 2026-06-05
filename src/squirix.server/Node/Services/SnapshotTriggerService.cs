using System;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

internal sealed class SnapshotTriggerService<T> : BackgroundService, ISnapshotReadinessStatus
{
    private readonly SnapshotCoordinator<T> _coordinator;

    private readonly IJournalCoordinator _journal;
    private readonly ILogger<SnapshotTriggerService<T>> _log;
    private readonly SnapshotTriggerOptions _opt;

    private readonly Channel<bool> _snapshotRequests = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
        });

    private int _fatalFailure;

    public SnapshotTriggerService(ILogger<SnapshotTriggerService<T>> log, SnapshotCoordinator<T> coordinator, IJournalCoordinator journal, SnapshotTriggerOptions opt)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _opt = opt ?? throw new ArgumentNullException(nameof(opt));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
    }

    public bool HasFatalFailure => Volatile.Read(ref _fatalFailure) != 0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!_opt.Enabled)
        {
            SnapshotTriggerLogs.LogDisabled(_log);
            return;
        }

        _journal.OnAppended += OnJournalAppended;
        SnapshotTriggerLogs.LogStarted(_log, 1);

        try
        {
            var period = TimeSpan.FromSeconds(1);
            while (!stoppingToken.IsCancellationRequested)
            {
                var requestTask = _snapshotRequests.Reader.WaitToReadAsync(stoppingToken).AsTask();
                var tickTask = Task.Delay(period, stoppingToken);
                var completed = await Task.WhenAny(requestTask, tickTask).ConfigureAwait(false);
                if (completed == requestTask)
                {
                    if (!await requestTask.ConfigureAwait(false))
                        break;

                    while (_snapshotRequests.Reader.TryRead(out _))
                    {
                        // Intentionally empty: coalesce bursty snapshot requests into one run.
                    }
                }

                if (_log.IsEnabled(LogLevel.Trace))
                    SnapshotTriggerLogs.LogTick(_log);

                await _coordinator.TrySnapshotAsync(_journal, stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            SnapshotTriggerLogs.LogCancelled(_log);
        }
        catch (Exception ex)
        {
            Volatile.Write(ref _fatalFailure, 1);
            SnapshotTriggerLogs.LogCrashed(_log, ex);
            throw;
        }
        finally
        {
            _journal.OnAppended -= OnJournalAppended;
            _ = _snapshotRequests.Writer.TryComplete();
            SnapshotTriggerLogs.LogStopped(_log);
        }

        return;

        void OnJournalAppended()
        {
            if (_log.IsEnabled(LogLevel.Trace))
                SnapshotTriggerLogs.LogJournalAppended(_log);

            _ = _snapshotRequests.Writer.TryWrite(true);
        }
    }
}
