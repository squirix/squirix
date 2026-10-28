using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Squirix.Server.Benchmarks;

/// <summary>Allocation cost of one wake of the snapshot trigger loop: a parked wait on the single-reader request channel, ended by a write.</summary>
[MemoryDiagnoser]
public class SnapshotRequestWaitBenchmarks
{
    private const int OperationsPerInvoke = 10_000;
    private readonly CancellationTokenSource _cancellation = new();

    private readonly Channel<bool> _requests = Channel.CreateBounded<bool>(
        new BoundedChannelOptions(1)
        {
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
        });

    /// <summary>Gets or sets a value indicating whether the wait passes a token that can cancel.</summary>
    [Params(false, true)]
    public bool CancelableToken { get; set; }

    /// <summary>Disposes the cancellation source.</summary>
    [GlobalCleanup]
    public void Cleanup() => _cancellation.Dispose();

    /// <summary>Parks a wait on the channel, then ends it with a write and drains the request.</summary>
    /// <returns>A task that completes when all operations finish.</returns>
    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public async Task WakeAsync()
    {
        var token = CancelableToken ? _cancellation.Token : CancellationToken.None;
        for (var i = 0; i < OperationsPerInvoke; i++)
        {
            var wait = _requests.Reader.WaitToReadAsync(token);
            _ = _requests.Writer.TryWrite(true);
            _ = await wait.ConfigureAwait(false);
            _ = _requests.Reader.TryRead(out _);
        }
    }
}
