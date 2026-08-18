using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Squirix.Attributes;

namespace Squirix.Server.Node.Observability;

/// <summary>Utilities and interceptors for structured logging scopes and trace-context propagation.</summary>
internal static class Correlation
{
    internal const string TraceParentHeader = "traceparent";
    internal const string TraceStateHeader = "tracestate";

    internal static IDisposable BeginStandardScope(ILogger logger, string nodeId, string? method = null)
    {
        // Capture Activity by reference and format ids only if a scope provider enumerates state.
        var scope = logger.BeginScope(new StandardScopeState(Activity.Current, nodeId, method));
        return scope ?? NoopDisposable.Instance;
    }

    [Immutable]
    private sealed class NoopDisposable : IDisposable
    {
        internal static readonly NoopDisposable Instance = new();

        void IDisposable.Dispose()
        {
        }
    }

    private sealed class StandardScopeEnumerator : IEnumerator<KeyValuePair<string, object?>>
    {
        private readonly string? _method;
        private readonly string _nodeId;
        private readonly string _spanId;
        private readonly string _traceId;
        private int _index;

        internal StandardScopeEnumerator(string traceId, string spanId, string nodeId, string? method)
        {
            _traceId = traceId;
            _spanId = spanId;
            _nodeId = nodeId;
            _method = method;
            _index = 0;
            Current = default;
        }

        public KeyValuePair<string, object?> Current { get; private set; }

        object IEnumerator.Current => Current;

        public void Dispose()
        {
        }

        public bool MoveNext()
        {
            switch (_index++)
            {
                case 0:
                    Current = new KeyValuePair<string, object?>("trace_id", _traceId);
                    return true;
                case 1:
                    Current = new KeyValuePair<string, object?>("span_id", _spanId);
                    return true;
                case 2:
                    Current = new KeyValuePair<string, object?>("node_id", _nodeId);
                    return true;
                case 3 when _method != null:
                    Current = new KeyValuePair<string, object?>("rpc.method", _method);
                    return true;
                default:
                    return false;
            }
        }

        public void Reset() => _index = 0;
    }

    [Immutable]
    private sealed class StandardScopeState : IReadOnlyList<KeyValuePair<string, object?>>
    {
        private readonly Activity? _activity;
        private readonly string? _method;
        private readonly string _nodeId;

        internal StandardScopeState(Activity? activity, string nodeId, string? method)
        {
            _activity = activity;
            _nodeId = nodeId;
            _method = method;
        }

        public int Count => _method == null ? 3 : 4;

        public KeyValuePair<string, object?> this[int index] =>
            index switch
            {
                0 => new KeyValuePair<string, object?>("trace_id", FormatTraceId(_activity)),
                1 => new KeyValuePair<string, object?>("span_id", FormatSpanId(_activity)),
                2 => new KeyValuePair<string, object?>("node_id", _nodeId),
                3 when _method != null => new KeyValuePair<string, object?>("rpc.method", _method),
                _ => throw new ArgumentOutOfRangeException(nameof(index)),
            };

        IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() =>
            new StandardScopeEnumerator(FormatTraceId(_activity), FormatSpanId(_activity), _nodeId, _method);

        IEnumerator IEnumerable.GetEnumerator() => new StandardScopeEnumerator(FormatTraceId(_activity), FormatSpanId(_activity), _nodeId, _method);

        private static string FormatSpanId(Activity? activity) => activity == null ? string.Empty : activity.SpanId.ToString();

        private static string FormatTraceId(Activity? activity) => activity == null ? string.Empty : activity.TraceId.ToString();
    }
}
