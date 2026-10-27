using System;
using System.IO;
using System.Security.Cryptography;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Benchmarks;

/// <summary>Background writers that keep the disk under test busy with large writes and periodic flushes, as a noisy neighbour on the same volume.</summary>
[Mutable]
internal sealed class DiskNoise : IDisposable
{
    private const int ChunkBytes = 4 * 1024 * 1024;
    private const int ChunksPerFlush = 8;
    private const long FileBytes = 512L * 1024 * 1024;
    private readonly CancellationTokenSource _stop = new();
    private readonly Thread[] _threads;

    private DiskNoise(string root, int writers)
    {
        _threads = new Thread[writers];
        for (var i = 0; i < writers; i++)
        {
            var path = Path.Join(root, "squirix-disk-noise-" + Guid.NewGuid().ToString("N") + ".bin");
            var worker = new Worker(path, _stop.Token);
            _threads[i] = new Thread(worker.Run) { IsBackground = true, Name = "journal-measure-disk-noise" };
            _threads[i].Start();
        }
    }

    /// <summary>Stops the writers and deletes their files.</summary>
    public void Dispose()
    {
        _stop.Cancel();
        foreach (var thread in _threads)
            thread.Join();

        _stop.Dispose();
    }

    /// <summary>Starts <paramref name="writers" /> noise writers under <paramref name="root" />; zero writers is a no-op.</summary>
    /// <param name="root">Directory on the disk under test.</param>
    /// <param name="writers">Number of concurrent noise writers.</param>
    /// <returns>The running noise source.</returns>
    internal static DiskNoise Start(string root, int writers) => new(root, writers);

    private sealed class Worker
    {
        private readonly string _path;
        private readonly CancellationToken _stop;

        internal Worker(string path, CancellationToken stop)
        {
            _path = path;
            _stop = stop;
        }

        internal void Run()
        {
            var chunk = new byte[ChunkBytes];
            RandomNumberGenerator.Fill(chunk);
            try
            {
                using var handle = File.OpenHandle(_path, FileMode.Create, FileAccess.Write, FileShare.None);
                long offset = 0;
                var sinceFlush = 0;
                while (!_stop.IsCancellationRequested)
                {
                    RandomAccess.Write(handle, chunk, offset);
                    offset = (offset + ChunkBytes) % FileBytes;
                    if (++sinceFlush < ChunksPerFlush)
                        continue;

                    RandomAccess.FlushToDisk(handle);
                    sinceFlush = 0;
                }
            }
            finally
            {
                File.Delete(_path);
            }
        }
    }
}
