using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
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
    private const int MaxBackoffExponent = 10;

    private const int MaxBackoffSeconds = 60;

    private const int MinBackoffMilliseconds = 1;

    private const int TickPeriodSeconds = 1;

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

    private int _consecutiveFailures;

    private SnapshotFailure? _currentFailure;

    private int _fatalFailure;

    public SnapshotTriggerService(ILogger<SnapshotTriggerService<T>> log, Coordinator coordinator, IJournalCoordinator journal, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(coordinator);
        ArgumentNullException.ThrowIfNull(log);
        ArgumentNullException.ThrowIfNull(journal);
        _coordinator = coordinator;
        _log = log;
        _journal = journal;
        _timeProvider = timeProvider;
        _onJournalAppended = OnJournalAppended;
    }

    public SnapshotFailure? CurrentFailure => Volatile.Read(ref _currentFailure);

    public bool HasFatalFailure => Volatile.Read(ref _fatalFailure) != 0;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _journal.OnAppended += _onJournalAppended;
        ServerLog.SnapshotTriggerStarted(_log, TickPeriodSeconds);

        try
        {
            await RunSnapshotLoopAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // Host shutdown; the finally block reports the stop.
        }
        finally
        {
            _journal.OnAppended -= _onJournalAppended;
            _ = _snapshotRequests.Writer.TryComplete();
            ServerLog.SnapshotTriggerStopped(_log);
        }
    }

    private static TimeSpan ComputeBackoff(int consecutiveFailures)
    {
        // Exponential backoff with full jitter: a uniform delay between the minimum and the capped exponential.
        var exponent = Math.Min(consecutiveFailures, MaxBackoffExponent);
        var maxMilliseconds = Math.Min(MaxBackoffSeconds, Math.Pow(2, exponent)) * 1000d;
        var jitter = RandomNumberGenerator.GetInt32(0, int.MaxValue) * (maxMilliseconds / int.MaxValue);
        return TimeSpan.FromMilliseconds(Math.Max(MinBackoffMilliseconds, jitter));
    }

    private static bool IsRetriable(Exception ex) =>
        ex is IOException or UnauthorizedAccessException or InvalidDataException or InvalidOperationException or ObjectDisposedException or OperationCanceledException;

    private void OnJournalAppended(object? sender, EventArgs e)
    {
        if (_log.IsEnabled(LogLevel.Trace))
            ServerLog.SnapshotTriggerJournalAppended(_log);

        _ = _snapshotRequests.Writer.TryWrite(true);
    }

    private SnapshotFailure PublishFailure(Exception ex)
    {
        var failure = new SnapshotFailure(ex.GetType().Name, ex.Message, ++_consecutiveFailures, _timeProvider.GetUtcNow());
        Volatile.Write(ref _currentFailure, failure);
        return failure;
    }

    private void RecordFatalCrash(Exception ex)
    {
        _ = PublishFailure(ex);
        Volatile.Write(ref _fatalFailure, 1);
        ServerLog.SnapshotTriggerCrashed(_log, ex);
    }

    private void RecordSuccess()
    {
        if (_consecutiveFailures == 0)
            return;

        _consecutiveFailures = 0;
        Volatile.Write(ref _currentFailure, null);
    }

    /// <summary>Runs one snapshot attempt; a failure the node can outlive backs off and returns, any other failure latches the service.</summary>
    /// <param name="stoppingToken">The host stop token.</param>
    /// <returns><see langword="false" /> when the service latched fatally and the loop must end.</returns>
    private async Task<bool> TrySnapshotAsync(CancellationToken stoppingToken)
    {
        try
        {
            // A tick the triggers skip, or an attempt that publishes nothing, proves nothing about the failure.
            if (await _coordinator.SnapshotAsync(_journal, stoppingToken).ConfigureAwait(false))
                RecordSuccess();

            return true;
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            throw;
        }
#pragma warning disable CA1031 // Any failure outside the retriable list latches the service fatally instead of stopping the host.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            // A latched journal pipeline cannot recover without a restart, and neither can a failure this service does not know.
            if (!IsRetriable(ex) || _journal.GetJournalThreadFailure() != null)
            {
                RecordFatalCrash(ex);
                return false;
            }

            var failure = PublishFailure(ex);
            var backoff = ComputeBackoff(failure.ConsecutiveFailures);
            ServerLog.SnapshotFailedRetrying(_log, ex, failure.ConsecutiveFailures, Convert.ToInt32(backoff.TotalMilliseconds));
            await Task.Delay(backoff, _timeProvider, stoppingToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            return true;
        }
    }

    private async Task RunSnapshotLoopAsync(CancellationToken stoppingToken)
    {
        // The tick is one more snapshot request from a single one-shot timer, re-armed after every round so it fires a period after the
        // last one, as a fresh delay per round would; a journal append, which wakes the loop on every mutation, then costs no timer.
        var period = TimeSpan.FromSeconds(TickPeriodSeconds);
        var tick = _timeProvider.CreateTimer(
            static state => _ = Unsafe.As<ChannelWriter<bool>>(state)!.TryWrite(true),
            _snapshotRequests.Writer,
            period,
            Timeout.InfiniteTimeSpan);
        await using var tickDisposal = tick.ConfigureAwait(false);
        while (await _snapshotRequests.Reader.WaitToReadAsync(stoppingToken).ConfigureAwait(false))
        {
            while (_snapshotRequests.Reader.TryRead(out _))
            {
                // Intentionally empty: coalesce bursty snapshot requests into one run.
            }

            if (_log.IsEnabled(LogLevel.Trace))
                ServerLog.SnapshotTriggerTick(_log);

            if (!await TrySnapshotAsync(stoppingToken).ConfigureAwait(false))
                break;

            _ = tick.Change(period, Timeout.InfiniteTimeSpan);
        }
    }
}
