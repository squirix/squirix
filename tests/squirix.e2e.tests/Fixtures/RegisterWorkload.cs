using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Squirix.E2ETests.Fixtures;

/// <summary>
/// Drives a set of single-writer registers through the SDK and records every call in a <see cref="RegisterHistory" />: one writer per key
/// writes the integers from one upwards, while one reader per key reads it through a second client.
/// </summary>
/// <remarks>
/// The workload is bounded by its operation counts, not by time. A write that fails with an RPC status or an unknown commit outcome is recorded as ambiguous and the
/// writer goes on with the next value; a read that fails with an RPC status is counted. Any other failure, the cancellation of the test
/// token included, ends the workload with that exception.
/// </remarks>
internal sealed class RegisterWorkload
{
    private readonly string[] _keys;
    private readonly ICache<long> _reader;
    private readonly ICache<long> _writer;

    /// <summary>Initializes a new instance of the <see cref="RegisterWorkload" /> class.</summary>
    /// <param name="writer">The cache the writers write through.</param>
    /// <param name="reader">The cache the readers read through, opened on a second client.</param>
    /// <param name="keys">The register keys; each must be absent when the workload starts.</param>
    /// <exception cref="ArgumentNullException">An argument is <see langword="null" />.</exception>
    /// <exception cref="ArgumentException"><paramref name="keys" /> is empty.</exception>
    internal RegisterWorkload(ICache<long> writer, ICache<long> reader, IReadOnlyCollection<string> keys)
    {
        ArgumentNullException.ThrowIfNull(writer);
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(keys);
        if (keys.Count == 0)
            throw new ArgumentException("At least one register key is required.", nameof(keys));

        _writer = writer;
        _reader = reader;
        _keys = [.. keys];
    }

    /// <summary>Gets the history the workload records into.</summary>
    internal RegisterHistory History { get; } = new();

    /// <summary>Runs the writers and readers of every key at once and completes when all of them have run their operations.</summary>
    /// <param name="writesPerKey">The number of writes of each key.</param>
    /// <param name="readsPerKey">The number of reads of each key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when every operation has run.</returns>
    /// <exception cref="ArgumentOutOfRangeException">A count is negative.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal Task RunAsync(int writesPerKey, int readsPerKey, CancellationToken cancellationToken)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(writesPerKey);
        ArgumentOutOfRangeException.ThrowIfNegative(readsPerKey);
        var calls = new Task[_keys.Length * 2];
        for (var i = 0; i < _keys.Length; i++)
        {
            calls[2 * i] = WriteAsync(_keys[i], writesPerKey, cancellationToken);
            calls[(2 * i) + 1] = ReadAsync(_keys[i], readsPerKey, cancellationToken);
        }

        return Task.WhenAll(calls);
    }

    private async Task ReadAsync(string key, int count, CancellationToken cancellationToken)
    {
        await Task.Yield();
        for (var i = 0; i < count; i++)
        {
            var start = Stopwatch.GetTimestamp();
            try
            {
                var read = await _reader.GetValueAsync(key, cancellationToken);
                History.RecordRead(new RegisterRead(key, start, Stopwatch.GetTimestamp(), read.Found ? read.Value : 0L));
            }
            catch (RpcException)
            {
                History.RecordFailedRead();
            }
        }
    }

    private async Task WriteAsync(string key, int count, CancellationToken cancellationToken)
    {
        await Task.Yield();
        for (var value = 1L; value <= count; value++)
        {
            var start = Stopwatch.GetTimestamp();
            var acked = false;
            try
            {
                await _writer.SetAsync(key, value, cancellationToken: cancellationToken);
                acked = true;
            }
            catch (Exception exception) when (exception is RpcException or CommitOutcomeUnknownException)
            {
                // The write may or may not have taken effect: the history keeps it as ambiguous.
            }

            History.RecordWrite(new RegisterWrite(key, value, start, Stopwatch.GetTimestamp(), acked));
        }
    }
}
