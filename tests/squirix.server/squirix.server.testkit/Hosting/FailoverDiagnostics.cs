using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Logging;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
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
    private readonly ConcurrentQueue<((int Client, int Server) Ports, double At, ProxyDirection Direction, int Size, bool Forwarded)> _proxyEvents = new();
    private readonly string _group;
    private readonly Action<string> _output;
    private readonly string _testName;
    private readonly long _started = Stopwatch.GetTimestamp();
    private readonly ManualResetEventSlim _stopSampling = new();
    private readonly Task _sampler;

    internal FailoverDiagnostics(string group, string testName, Action<string> output)
    {
        _group = group;
        _output = output;
        _testName = testName;
        ThreadPool.GetMinThreads(out var minimum, out var ioMinimum);
        if (int.TryParse(Environment.GetEnvironmentVariable("SQUIRIX_TRACE_MIN_WORKERS"), CultureInfo.InvariantCulture, out var requested) && requested > minimum)
            _ = ThreadPool.SetMinThreads(requested, ioMinimum);

        ThreadPool.GetMinThreads(out minimum, out _);
        Record($"thread pool configuration processors={Environment.ProcessorCount}, minWorkers={minimum}");
        _sampler = Task.Factory.StartNew(SampleThreadPool, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        PreVoteTrace.Observed += RecordTrace;
        PreVoteTrace.TlsObserved += RecordTls;
        TcpPartitionProxy.BridgeObserved += RecordBridge;
        TcpPartitionProxy.BytesObserved += RecordProxyBytes;
    }

    public async ValueTask DisposeAsync()
    {
        _stopSampling.Set();
        await _sampler.ConfigureAwait(false);
        _stopSampling.Dispose();
        PreVoteTrace.Observed -= RecordTrace;
        PreVoteTrace.TlsObserved -= RecordTls;
        TcpPartitionProxy.BridgeObserved -= RecordBridge;
        TcpPartitionProxy.BytesObserved -= RecordProxyBytes;
        var directory = Environment.GetEnvironmentVariable("SQUIRIX_FAILOVER_TRACE_DIR");
        if (string.IsNullOrEmpty(directory))
        {
            WriteEvents(_output);
            return;
        }

        _ = Directory.CreateDirectory(directory);
        await using var writer = File.CreateText(Path.Join(directory, _testName + ".log"));
        WriteEvents(writer.WriteLine);
        foreach (var entry in _events)
        {
            if (!entry.Contains(" peer trace=", StringComparison.Ordinal))
                _output($"[failover {_group}] {entry}");
        }
    }

    internal void Register(string nodeId, TestServiceCollection services)
    {
        _ = services.RemoveAll<IServerLoggerFactory>().AddSingleton<IServerLoggerFactory>(new ElectionLogger(this, nodeId));
        _ = services.RemoveAll<IReplicaRpcGateway>().AddSingleton<IReplicaRpcGateway>(sp => new AppendGateway(sp.GetRequiredService<ReplicaRpcGateway>(), this));
        new ElectionRoundCounter { ObservePreVoteResult = PreVoteCompleted, Observe = ElectionRequest, TracePreVote = StartPreVote }.Register(services);
    }

    internal void Record(string message) => _events.Enqueue($"{Stopwatch.GetElapsedTime(_started).TotalMilliseconds:F0} ms {message}");

    private void WriteEvents(Action<string> output)
    {
        foreach (var entry in _events)
            output($"[failover {_group}] {entry}");

        foreach (var (ports, at, direction, size, forwarded) in _proxyEvents)
            output($"[failover {_group}] {at:F0} ms proxy socket={ports.Client}:{ports.Server} direction={direction} phase={(forwarded ? "forwarded" : "read")} bytes={size}");

        foreach (var (ports, at, phase) in _tlsEvents)
        {
            var senderPorts = _bridges.GetValueOrDefault(ports, ports);
            if (_connections.TryGetValue(senderPorts, out var traceId) && _traces.TryGetValue(traceId, out var peer))
                output($"[failover {_group}] {at:F0} ms peer trace={traceId} {peer} {phase}");
        }
    }

    private void RecordProxyBytes(object? sender, TcpPartitionProxy.BytesEventArgs stage)
    {
        if (_connections.ContainsKey(stage.Ports))
            _proxyEvents.Enqueue((stage.Ports, Stopwatch.GetElapsedTime(_started).TotalMilliseconds, stage.Direction, stage.Size, stage.Forwarded));
    }

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

        var socket = stage.Ports == default ? string.Empty : $" socket={stage.Ports.Client}:{stage.Ports.Server}";
        Record($"peer trace={stage.TraceId} {peer} {stage.Phase}{socket}");
    }

    private void RecordTls(object? sender, PreVoteTrace.TlsEventArgs stage) =>
        _tlsEvents.Enqueue((stage.Ports, Stopwatch.GetElapsedTime(_started).TotalMilliseconds, stage.Phase));

    private void RecordBridge(object? sender, TcpPartitionProxy.BridgeEventArgs bridge) =>
        _ = _bridges.TryAdd(bridge.Upstream, bridge.Downstream);

    private Activity? StartPreVote(string groupId, string sender, string target, ulong term) =>
        string.Equals(groupId, _group, StringComparison.Ordinal)
            ? StartTrace(PreVoteTrace.Operation, $"{sender} -> {target} term={term}")
            : null;

    private Activity StartTrace(string operation, string label)
    {
        var trace = new Activity(operation);
        _ = trace.SetParentId(ActivityTraceId.CreateRandom(), ActivitySpanId.CreateRandom(), ActivityTraceFlags.Recorded);
        _ = trace.Start();
        _ = _traces.TryAdd(trace.TraceId, label);
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

    [Immutable]
    private sealed class AppendGateway : IReplicaRpcGateway
    {
        private readonly IReplicaRpcGateway _inner;
        private readonly FailoverDiagnostics _diagnostics;

        internal AppendGateway(IReplicaRpcGateway inner, FailoverDiagnostics diagnostics)
        {
            _inner = inner;
            _diagnostics = diagnostics;
        }

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (!string.Equals(header.GroupId, _diagnostics._group, StringComparison.Ordinal))
                return await _inner.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);

            var kind = batch.Entries.Count == 0 ? "heartbeat" : "append";
            using var trace = _diagnostics.StartTrace(PreVoteTrace.AppendOperation, $"{header.SenderNodeId} -> {nodeId} term={header.Term} {kind}");
            PreVoteTrace.Mark(trace.TraceId, $"send append start entries={batch.Entries.Count} previous={batch.PrevLogIndex} commit={batch.LeaderCommitIndex}");
            try
            {
                var reply = await _inner.AppendEntriesAsync(nodeId, header, batch, cancellationToken).ConfigureAwait(false);
                PreVoteTrace.Mark(trace.TraceId, $"send append result success={reply.Success} term={reply.CurrentTerm} refusal={reply.RefusalCode} last={reply.LastLogIndex}");
                return reply;
            }
            catch (Exception error)
            {
                PreVoteTrace.Mark(trace.TraceId, $"send append failure {error.GetType().Name}: {error.Message}; cause={error.InnerException?.GetType().Name}: {error.InnerException?.Message}");
                throw;
            }
        }
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
