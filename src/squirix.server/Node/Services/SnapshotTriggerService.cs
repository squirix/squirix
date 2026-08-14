using System;
using System.IO;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

internal sealed class SnapshotTriggerService<T> : BackgroundService, ISnapshotReadinessStatus
{
    private readonly Coordinator _coordinator;

    private readonly IJournalCoordinator _journal;
    private readonly ILogger<SnapshotTriggerService<T>> _log;

    private readonly EventHandler _onJournalAppended;

    private readonly Channel<bool> _snapshotRequests = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
        });

    private readonly TimeProvider _timeProvider;

    private int _fatalFailure;

    public SnapshotTriggerService(ILogger<SnapshotTriggerService<T>> log, Coordinator coordinator, IJournalCoordinator journal, TimeProvider? timeProvider = null)
    {
        _coordinator = coordinator ?? throw new ArgumentNullException(nameof(coordinator));
        _log = log ?? throw new ArgumentNullException(nameof(log));
        _journal = journal ?? throw new ArgumentNullException(nameof(journal));
        _timeProvider = timeProvider ?? TimeProvider.System;
        _onJournalAppended = OnJournalAppended;
    }

    public bool HasFatalFailure => Volatile.Read(ref _fatalFailure) is not 0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _journal.OnAppended += _onJournalAppended;
        LogManager.SnapshotTriggerStarted(_log, 1);

        try
        {
            await RunSnapshotLoopAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            LogManager.SnapshotTriggerCanceled(_log);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or UnauthorizedAccessException)
        {
            RecordFatalCrash(ex);
            throw;
        }
        finally
        {
            _journal.OnAppended -= _onJournalAppended;
            _ = _snapshotRequests.Writer.TryComplete();
            LogManager.SnapshotTriggerStopped(_log);
        }
    }

    private void OnJournalAppended(object? sender, EventArgs e)
    {
        if (_log.IsEnabled(LogLevel.Trace))
            LogManager.SnapshotTriggerJournalAppended(_log);

        _ = _snapshotRequests.Writer.TryWrite(true);
    }

    private void RecordFatalCrash(Exception ex)
    {
        Volatile.Write(ref _fatalFailure, 1);
        LogManager.SnapshotTriggerCrashed(_log, ex);
    }

    private async Task RunSnapshotLoopAsync(CancellationToken stoppingToken)
    {
        var period = TimeSpan.FromSeconds(1);
        while (!stoppingToken.IsCancellationRequested)
        {
            var requestTask = _snapshotRequests.Reader.WaitToReadAsync(stoppingToken).AsTask();
            var tickTask = Task.Delay(period, _timeProvider, stoppingToken);
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
                LogManager.SnapshotTriggerTick(_log);

            await _coordinator.TrySnapshotAsync(_journal, stoppingToken).ConfigureAwait(false);
        }
    }
}
