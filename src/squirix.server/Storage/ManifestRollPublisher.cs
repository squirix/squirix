using System;
using System.Collections.Concurrent;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;

namespace Squirix.Server.Storage;

/// <summary>Publishes journal roll metadata on a dedicated thread so WAL I/O does not block on manifest disk writes.</summary>
internal sealed class ManifestRollPublisher : IDisposable
{
    private readonly Action<Exception>? _onRollFailed;
    private readonly ManifestStore _manifestStore;
    private readonly BlockingCollection<ManifestRollRequest> _queue = new();
    private readonly Thread _thread;
    private int _disposed;
    private int _inFlight;

    public ManifestRollPublisher(ManifestStore manifestStore, Action<Exception>? onRollFailed = null)
    {
        _manifestStore = manifestStore ?? throw new ArgumentNullException(nameof(manifestStore));
        _onRollFailed = onRollFailed;
        _thread = new Thread(ProcessQueue) { IsBackground = true, Name = "squirix-manifest-roll" };
        _thread.Start();
    }

    public void PublishRollAndWait(int currentJournal, ulong nextSequence)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is 1, this);

        using var done = new ManualResetEventSlim(false);
        Exception? failure = null;
        if (!_queue.TryAdd(new ManifestRollRequest(currentJournal, nextSequence, done, ex => failure = ex), TimeSpan.FromSeconds(30)))
            throw new InvalidOperationException("manifest roll publisher is shutting down.");

        if (!done.Wait(TimeSpan.FromSeconds(5), CancellationToken.None))
            throw new TimeoutException("manifest roll did not complete within 5 seconds.");
        if (failure is not null)
            ExceptionDispatchInfo.Capture(failure).Throw();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        _queue.CompleteAdding();
        _ = _thread.Join(TimeSpan.FromSeconds(30));
        _queue.Dispose();
    }

    private void ProcessQueue()
    {
        try
        {
            foreach (var request in _queue.GetConsumingEnumerable(CancellationToken.None))
            {
                _ = Interlocked.Increment(ref _inFlight);
                try
                {
                    _manifestStore.PublishRollBlocking(request.CurrentJournal, request.NextSequence);
                }
                catch (Exception ex) when (ex is IOException or InvalidOperationException or InvalidDataException)
                {
                    request.CaptureFailure(ex);
                    _onRollFailed?.Invoke(ex);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref _inFlight);
                    request.SignalComplete();
                }
            }
        }
        catch (InvalidOperationException)
        {
            // Queue completed during shutdown.
        }
    }

    private sealed class ManifestRollRequest(int currentJournal, ulong nextSequence, ManualResetEventSlim done, Action<Exception> captureFailure)
    {
        public int CurrentJournal { get; } = currentJournal;

        public ulong NextSequence { get; } = nextSequence;

        public void CaptureFailure(Exception ex) => captureFailure(ex);

        public void SignalComplete() => done.Set();
    }
}
