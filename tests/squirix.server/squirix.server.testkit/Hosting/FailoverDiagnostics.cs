using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Logging;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

internal sealed class FailoverDiagnostics : IAsyncDisposable
{
    private readonly ConcurrentQueue<string> _events = new();
    private readonly ConcurrentDictionary<ActivityTraceId, string> _traces = new();
    private readonly ConcurrentDictionary<(int Client, int Server), ActivityTraceId> _connections = new();
    private readonly ConcurrentDictionary<(int Client, int Server), (int Client, int Server)> _bridges = new();
    private readonly ConcurrentQueue<((int Client, int Server) Ports, double At, string Phase)> _tlsEvents = new();
    private readonly string _group;
    private readonly Action<string> _output;
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly ManualResetEventSlim _stopSampling = new();
    private readonly Task _sampler;

    internal FailoverDiagnostics(string group, Action<string> output)
    {
        _group = group;
        _output = output;
        ThreadPool.GetMinThreads(out var minimum, out var ioMinimum);
        if (int.TryParse(Environment.GetEnvironmentVariable("SQUIRIX_TRACE_MIN_WORKERS"), CultureInfo.InvariantCulture, out var requested) && requested > minimum)
            _ = ThreadPool.SetMinThreads(requested, ioMinimum);

        ThreadPool.GetMinThreads(out minimum, out _);
        Record($"thread pool configuration processors={Environment.ProcessorCount}, minWorkers={minimum}");
        _sampler = Task.Factory.StartNew(SampleThreadPool, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        PreVoteTrace.Observed += RecordTrace;
        PreVoteTrace.TlsObserved += RecordTls;
        TcpPartitionProxy.BridgeObserved += RecordBridge;
    }

    public async ValueTask DisposeAsync()
    {
        _stopSampling.Set();
        await _sampler.ConfigureAwait(false);
        _stopSampling.Dispose();
        PreVoteTrace.Observed -= RecordTrace;
        PreVoteTrace.TlsObserved -= RecordTls;
        TcpPartitionProxy.BridgeObserved -= RecordBridge;
        foreach (var entry in _events)
            _output($"[failover {_group}] {entry}");

        foreach (var (ports, at, phase) in _tlsEvents)
        {
            var senderPorts = _bridges.GetValueOrDefault(ports, ports);
            if (_connections.TryGetValue(senderPorts, out var traceId) && _traces.TryGetValue(traceId, out var peer))
                _output($"[failover {_group}] {at:F0} ms pre-vote trace={traceId} {peer} {phase}");
        }
    }

    internal void Register(string nodeId, TestServiceCollection services)
    {
        _ = services.RemoveAll<IServerLoggerFactory>().AddSingleton<IServerLoggerFactory>(new ElectionLogger(this, nodeId));
        new ElectionRoundCounter { ObservePreVoteResult = PreVoteCompleted, Observe = ElectionRequest, TracePreVote = StartPreVote }.Register(services);
    }

    internal void Record(string message) => _events.Enqueue($"{Stopwatch.GetElapsedTime(_started).TotalMilliseconds:F0} ms {message}");

    private void SampleThreadPool()
    {
        while (!_stopSampling.Wait(200, CancellationToken.None))
        {
            var queued = Stopwatch.GetTimestamp();
            var pending = ThreadPool.PendingWorkItemCount;
            var threads = ThreadPool.ThreadCount;
            _ = ThreadPool.UnsafeQueueUserWorkItem(
                static state =>
                {
                    var delay = Stopwatch.GetElapsedTime(state.Queued);
                    if (delay >= TimeSpan.FromMilliseconds(50))
                        state.Owner.Record($"thread pool sentinel delay={delay.TotalMilliseconds:F0} ms, pendingAtQueue={state.Pending}, workersAtQueue={state.Threads}");
                },
                (Owner: this, Queued: queued, Pending: pending, Threads: threads),
                false);
        }
    }

    private void RecordTrace(object? sender, PreVoteTrace.StageEventArgs stage)
    {
        if (!_traces.TryGetValue(stage.TraceId, out var peer))
            return;

        if (stage.Ports != default)
            _ = _connections.TryAdd(stage.Ports, stage.TraceId);

        Record($"pre-vote trace={stage.TraceId} {peer} {stage.Phase}");
    }

    private void RecordTls(object? sender, PreVoteTrace.TlsEventArgs stage) =>
        _tlsEvents.Enqueue((stage.Ports, Stopwatch.GetElapsedTime(_started).TotalMilliseconds, stage.Phase));

    private void RecordBridge(object? sender, TcpPartitionProxy.BridgeEventArgs bridge) =>
        _ = _bridges.TryAdd(bridge.Upstream, bridge.Downstream);

    private Activity? StartPreVote(string groupId, string sender, string target, ulong term)
    {
        if (!string.Equals(groupId, _group, StringComparison.Ordinal))
            return null;

        var trace = new Activity(PreVoteTrace.Operation);
        _ = trace.SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        _ = trace.Start();
        _ = _traces.TryAdd(trace.TraceId, $"{sender} -> {target} term={term}");
        return trace;
    }

    private void ElectionRequest(string groupId, string sender, string target, string kind, ulong term)
    {
        if (string.Equals(groupId, _group, StringComparison.Ordinal))
            Record($"{sender} -> {target} {kind} term={term} started");
    }

    private void PreVoteCompleted(string groupId, string sender, string target, ulong term, string result)
    {
        if (string.Equals(groupId, _group, StringComparison.Ordinal))
            Record($"{sender} -> {target} pre-vote term={term} {result}");
    }

    private sealed class ElectionLogger : IServerLoggerFactory, IServerLogger
    {
        private readonly FailoverDiagnostics _diagnostics;
        private readonly string _nodeId;

        internal ElectionLogger(FailoverDiagnostics diagnostics, string nodeId)
        {
            _diagnostics = diagnostics;
            _nodeId = nodeId;
        }

        public bool IsScopeEnabled => false;

        public IServerLogger CreateLogger(string categoryName) =>
            categoryName.EndsWith(nameof(ReplicaElectionService), StringComparison.Ordinal) ? this : NullLogger.Instance;

        public void Dispose()
        {
        }

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(ServerLogLevel logLevel) => logLevel is >= ServerLogLevel.Debug and < ServerLogLevel.None;

        public void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var message = formatter(state, exception);
            if (message.StartsWith($"Replica group {_diagnostics._group} ", StringComparison.Ordinal) || exception != null)
                _diagnostics.Record($"{_nodeId} {eventId.Name} {message}{(exception == null ? string.Empty : $" {exception}")}");
        }
    }
}
