using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Benchmarks;

/// <summary>Closed-loop durable PUT writers that record the end-to-end latency of every mutation while recording is on.</summary>
[ThreadSafe]
internal sealed class TailWriters
{
    private readonly JournalMeasurementHost _host;
    private readonly byte[] _payload;
    private int _recording;

    /// <summary>Initializes a new instance of the <see cref="TailWriters" /> class.</summary>
    /// <param name="host">The journal host the writers append to.</param>
    /// <param name="payloadBytes">The PUT payload size.</param>
    internal TailWriters(JournalMeasurementHost host, int payloadBytes)
    {
        _host = host;
        _payload = new byte[payloadBytes];
        Array.Fill(_payload, Convert.ToByte('m'));
    }

    /// <summary>Starts or stops recording end-to-end latencies.</summary>
    /// <param name="recording">Whether latencies are recorded.</param>
    internal void SetRecording(bool recording) => Volatile.Write(ref _recording, recording ? 1 : 0);

    /// <summary>Appends durably in a loop until <paramref name="cancellationToken" /> is canceled; each mutation uses a fresh key.</summary>
    /// <param name="writerId">The writer number, part of the keys.</param>
    /// <param name="samples">Receives the end-to-end latency of each mutation while recording.</param>
    /// <param name="cancellationToken">Stops the loop.</param>
    /// <returns>An asynchronous operation.</returns>
    internal async Task RunAsync(int writerId, LatencySamples samples, CancellationToken cancellationToken)
    {
        await Task.Yield();
        var journal = _host.Journal;
        var sequence = 0L;
        while (!cancellationToken.IsCancellationRequested)
        {
            var key = new CacheKey("tail", string.Create(CultureInfo.InvariantCulture, $"w{writerId}-{sequence++}"));
            var start = TimeProvider.System.GetTimestamp();
            _ = await _host.Executor.ExecuteAsync(
                key,
                static (_, _) => ValueTask.FromResult(DurableMutationCondition<int>.Apply()),
                new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, ReadOnlyMemory<byte> Payload), int>(
                    (journal, key, _payload),
                    static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, s.Key, s.Payload, ct),
                    static (_, _) => new ValueTask<int>(1)),
                CancellationToken.None).ConfigureAwait(false);
            if (Volatile.Read(ref _recording) != 0)
                samples.Add(TimeProvider.System.GetElapsedTime(start));
        }
    }
}
