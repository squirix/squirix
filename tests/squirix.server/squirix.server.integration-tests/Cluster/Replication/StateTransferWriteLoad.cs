using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Closed-loop writers that set keys of a group through its owner and record the latency of every write and every refusal.</summary>
/// <remarks>Each write carries a fresh ASCII string of the configured length, so no node shares value memory with another; its payload is that many bytes in UTF-8.</remarks>
internal sealed class StateTransferWriteLoad
{
    private const string Letters = "abcdefghijklmnopqrstuvwxyz";

    private static readonly TimeSpan RefusalPause = TimeSpan.FromMilliseconds(1);
    private static readonly TimeSpan StallThreshold = TimeSpan.FromSeconds(1);

    private readonly ILogicalNamespacedCache<object?> _cache;
    private readonly string _cacheName;
    private readonly ConcurrentQueue<(TimeSpan At, TimeSpan Took)> _stalls = new();
    private readonly ConcurrentDictionary<string, int> _errors = new(StringComparer.Ordinal);
    private readonly string[] _keys;
    private readonly long _limit;
    private readonly (int Start, int Length) _segment;
    private readonly CancellationTokenSource _stop;
    private readonly int _valueBytes;
    private readonly int _workers;
    private List<int>[] _latencies = [];
    private long _next = -1;
    private long _started;
    private Task[] _tasks = [];

    /// <summary>Initializes a new instance of the <see cref="StateTransferWriteLoad" /> class.</summary>
    /// <param name="cache">The cache of the owner node.</param>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="keys">The keys the owner owns.</param>
    /// <param name="segment">The part of <paramref name="keys" /> the writers use; they wrap around it.</param>
    /// <param name="shape">The number of writers, the value size and the number of writes after which the writers end; zero for no limit.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    internal StateTransferWriteLoad(
        ILogicalNamespacedCache<object?> cache,
        string cacheName,
        string[] keys,
        (int Start, int Length) segment,
        (int Workers, int ValueBytes, long Limit) shape,
        CancellationToken cancellationToken)
    {
        _cache = cache;
        _cacheName = cacheName;
        _keys = keys;
        _segment = segment;
        (_workers, _valueBytes, _limit) = shape;
        _stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
    }

    /// <summary>Starts the writers.</summary>
    internal void Start()
    {
        _started = Stopwatch.GetTimestamp();
        _latencies = new List<int>[_workers];
        _tasks = new Task[_workers];
        for (var i = 0; i < _workers; i++)
        {
            var latencies = new List<int>();
            _latencies[i] = latencies;
            _tasks[i] = RunAsync(latencies);
        }
    }

    /// <summary>Waits until every writer has ended on its limit.</summary>
    /// <returns>The outcome.</returns>
    internal async Task<StateTransferWriteResult> CompleteAsync()
    {
        await Task.WhenAll(_tasks);
        _stop.Dispose();
        return Collect();
    }

    /// <summary>Stops the writers and returns what they did.</summary>
    /// <returns>The outcome.</returns>
    internal async Task<StateTransferWriteResult> StopAsync()
    {
        await _stop.CancelAsync();
        await Task.WhenAll(_tasks);
        _stop.Dispose();
        return Collect();
    }

    private StateTransferWriteResult Collect()
    {
        var elapsed = Stopwatch.GetElapsedTime(_started);
        var total = 0;
        foreach (var list in _latencies)
            total += list.Count;

        var all = new int[total];
        var at = 0;
        foreach (var list in _latencies)
        {
            list.CopyTo(all, at);
            at += list.Count;
        }

        Array.Sort(all);
        return new StateTransferWriteResult(all, new SortedDictionary<string, int>(_errors, StringComparer.Ordinal), elapsed, [.. _stalls]);
    }

    private async Task RunAsync(List<int> latencies)
    {
        await Task.Yield();
        var token = _stop.Token;
        while (!token.IsCancellationRequested)
        {
            var index = Interlocked.Increment(ref _next);
            if (_limit > 0 && index >= _limit)
                return;

            var key = _keys[_segment.Start + int.CreateChecked(index % _segment.Length)];
            var value = new string(Letters[int.CreateChecked(index % Letters.Length)], _valueBytes);
            var started = Stopwatch.GetTimestamp();
            if (await TrySetAsync(key, new NodeCacheEntry<object?> { Value = value, Version = index + 1 }, token))
                Record(latencies, started);
            else if (!token.IsCancellationRequested)
                await Task.Delay(RefusalPause, TimeProvider.System, CancellationToken.None);
        }
    }

    private void Record(List<int> latencies, long started)
    {
        var took = Stopwatch.GetElapsedTime(started);
        latencies.Add(int.CreateSaturating(took.TotalMicroseconds));
        if (took > StallThreshold)
            _stalls.Enqueue((Stopwatch.GetElapsedTime(_started, started), took));
    }

    private async Task<bool> TrySetAsync(string key, NodeCacheEntry<object?> entry, CancellationToken token)
    {
        try
        {
            await _cache.SetEntryAsync(Guid.NewGuid().ToString("N"), _cacheName, key, entry, token);
            return true;
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested)
        {
            return false;
        }
        catch (RpcException exception)
        {
            Count($"rpc:{exception.StatusCode}");
        }
        catch (SquirixException exception)
        {
            Count($"squirix:{exception.GetType().Name}");
        }
        catch (ResourceExhaustedException)
        {
            Count("memory-pressure-refusal");
        }
        catch (JournalCapacityExceededException)
        {
            Count("journal-capacity-refusal");
        }
        catch (TimeoutException)
        {
            Count("timeout");
        }

        return false;
    }

    private void Count(string kind) => _errors.AddOrUpdate(kind, 1, static (_, current) => current + 1);
}
