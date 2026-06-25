using System;
using System.Collections.Concurrent;
using System.IO;
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

    /// <summary>Enqueues a roll; <paramref name="onSuccess" /> runs on the manifest thread after a successful publish.</summary>
    /// <param name="currentJournal">Journal segment index being rolled to.</param>
    /// <param name="nextSequence">Next journal sequence after the roll.</param>
    /// <param name="onSuccess">Callback invoked after manifest publish succeeds.</param>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="onSuccess" /> is null.</exception>
    /// <exception cref="ObjectDisposedException">Thrown when the publisher has been disposed.</exception>
    /// <exception cref="InvalidOperationException">Thrown when the roll queue is shutting down.</exception>
    public void PublishRoll(int currentJournal, ulong nextSequence, Action onSuccess)
    {
        ArgumentNullException.ThrowIfNull(onSuccess);
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) is 1, this);

        if (!_queue.TryAdd(new ManifestRollRequest(currentJournal, nextSequence, onSuccess), TimeSpan.FromSeconds(30)))
            throw new InvalidOperationException("manifest roll publisher is shutting down.");
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
                    request.SignalSuccess();
                }
                catch (Exception ex) when (ex is IOException or InvalidOperationException or InvalidDataException)
                {
                    request.CaptureFailure(ex);
                    _onRollFailed?.Invoke(ex);
                }
                finally
                {
                    _ = Interlocked.Decrement(ref _inFlight);
                }
            }
        }
        catch (InvalidOperationException)
        {
            // Queue completed during shutdown.
        }
    }

    private sealed class ManifestRollRequest
    {
        private readonly Action? _onSuccess;
        private readonly Action<Exception>? _captureFailure;

        public ManifestRollRequest(int currentJournal, ulong nextSequence, Action onSuccess, Action<Exception>? captureFailure = null)
        {
            CurrentJournal = currentJournal;
            NextSequence = nextSequence;
            _onSuccess = onSuccess;
            _captureFailure = captureFailure;
        }

        public int CurrentJournal { get; }

        public ulong NextSequence { get; }

        public void CaptureFailure(Exception ex) => _captureFailure?.Invoke(ex);

        public void SignalSuccess() => _onSuccess?.Invoke();
    }
}
