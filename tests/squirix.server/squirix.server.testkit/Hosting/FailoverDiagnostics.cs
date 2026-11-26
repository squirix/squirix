using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using Squirix.Server.Logging;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

internal sealed class FailoverDiagnostics : IDisposable
{
    private readonly ConcurrentQueue<string> _events = new();
    private readonly ConcurrentDictionary<ActivityTraceId, string> _traces = new();
    private readonly ConcurrentDictionary<(int Client, int Server), ActivityTraceId> _connections = new();
    private readonly ConcurrentDictionary<(int Client, int Server), (int Client, int Server)> _bridges = new();
    private readonly ConcurrentQueue<((int Client, int Server) Ports, double At, string Phase)> _tlsEvents = new();
    private readonly string _group;
    private readonly Action<string> _output;
    private readonly long _started = Stopwatch.GetTimestamp();

    internal FailoverDiagnostics(string group, Action<string> output)
    {
        _group = group;
        _output = output;
        PreVoteTrace.Observed += RecordTrace;
        PreVoteTrace.TlsObserved += RecordTls;
        TcpPartitionProxy.BridgeObserved += RecordBridge;
    }

    public void Dispose()
    {
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
