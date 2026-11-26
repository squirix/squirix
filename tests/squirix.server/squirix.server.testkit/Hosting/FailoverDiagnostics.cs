using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using Squirix.Server.Logging;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit.Logging;

namespace Squirix.Server.TestKit.Hosting;

internal sealed class FailoverDiagnostics : IDisposable
{
    private readonly ConcurrentQueue<string> _events = new();
    private readonly string _group;
    private readonly Action<string> _output;
    private readonly long _started = Stopwatch.GetTimestamp();

    internal FailoverDiagnostics(string group, Action<string> output)
    {
        _group = group;
        _output = output;
    }

    public void Dispose()
    {
        foreach (var entry in _events)
            _output($"[failover {_group}] {entry}");
    }

    internal void Register(string nodeId, TestServiceCollection services)
    {
        _ = services.RemoveAll<IServerLoggerFactory>().AddSingleton<IServerLoggerFactory>(new ElectionLogger(this, nodeId));
        new ElectionRoundCounter { ObservePreVoteResult = PreVoteCompleted, Observe = ElectionRequest }.Register(services);
    }

    internal void Record(string message) => _events.Enqueue($"{Stopwatch.GetElapsedTime(_started).TotalMilliseconds:F0} ms {message}");

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
