using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

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

    public sealed class ClientInterceptor : Interceptor
    {
        private readonly ILogger<ClientInterceptor> _log;
        private readonly string _nodeId;

        public ClientInterceptor(ILogger<ClientInterceptor> log, ClusterConfig cluster)
        {
            _log = log;
            _nodeId = cluster.NodeId;
        }

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(
            TRequest request,
            ClientInterceptorContext<TRequest, TResponse> context,
            AsyncUnaryCallContinuation<TRequest, TResponse> continuation)
        {
            var callOptions = AttachTraceHeaders(context.Options, context.Method.FullName, out var ownedActivity);
            var ctx2 = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
            var scope = BeginStandardScope(_log, _nodeId, context.Method.FullName);
            var call = base.AsyncUnaryCall(request, ctx2, continuation);
            return WrapUnaryCallAsync(scope, ownedActivity, call);
        }

        private static CallOptions AttachTraceHeaders(CallOptions opt, string method, out Activity? ownedActivity)
        {
            var meta = opt.Headers ?? [];

            // Reuse the ambient Activity when present (the caller owns it); otherwise start a client Activity
            // to propagate. We own only the one we start, so its lifetime is tied to the outbound call below.
            ownedActivity = null;
            var activity = Activity.Current;
            if (activity is null)
            {
                activity = ActivitySourceHolder.StartClient(method);
                ownedActivity = activity;
            }

            if (activity is null)
                return new CallOptions(meta, opt.Deadline, opt.CancellationToken, opt.WriteOptions, opt.PropagationToken, opt.Credentials);

            var tp = activity.Id;
            if (!string.IsNullOrEmpty(tp))
                Upsert(meta, TraceParentHeader, tp);

            var ts = activity.TraceStateString;
            if (!string.IsNullOrEmpty(ts))
                Upsert(meta, TraceStateHeader, ts);

            return new CallOptions(meta, RpcDeadlineContext.EffectiveDeadline(opt.Deadline), opt.CancellationToken, opt.WriteOptions, opt.PropagationToken, opt.Credentials);
        }

        private static void Upsert(Metadata meta, string key, string value)
        {
            for (var i = 0; i < meta.Count; i++)
            {
                if (!string.Equals(meta[i].Key, key, StringComparison.Ordinal))
                    continue;

                meta.RemoveAt(i);
                break;
            }

            meta.Add(new Metadata.Entry(key, value));
        }

        private static AsyncUnaryCall<TResponse> WrapUnaryCallAsync<TResponse>(IDisposable scope, Activity? ownedActivity, AsyncUnaryCall<TResponse> inner)
        {
            var disposed = 0;

            async Task<TResponse> ResponseAsync()
            {
                try
                {
#pragma warning disable VSTHRD003

                    // Scope and owned client Activity must live until the outbound unary call completes.
                    return await inner.ResponseAsync.ConfigureAwait(false);
#pragma warning restore VSTHRD003
                }
                finally
                {
                    DisposeOnce();
                }
            }

            return new AsyncUnaryCall<TResponse>(
                ResponseAsync(),
                inner.ResponseHeadersAsync,
                inner.GetStatus,
                inner.GetTrailers,
                () =>
                {
                    DisposeOnce();
                    inner.Dispose();
                });

            void DisposeOnce()
            {
                if (Interlocked.Exchange(ref disposed, 1) is not 0)
                    return;
                scope.Dispose();
                ownedActivity?.Dispose();
            }
        }
    }

    public sealed class ServerInterceptor : Interceptor
    {
        private readonly ILogger<ServerInterceptor> _log;
        private readonly string _nodeId;

        public ServerInterceptor(ILogger<ServerInterceptor> log, ClusterConfig cluster)
        {
            _log = log;
            _nodeId = cluster.NodeId;
        }

        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(
            TRequest request,
            ServerCallContext context,
            UnaryServerMethod<TRequest, TResponse> continuation)
        {
            var headers = context.RequestHeaders;
            var tp = headers.GetValue(TraceParentHeader);
            var ts = headers.GetValue(TraceStateHeader);

            using var activity = StartServerActivity(tp, ts, context.Method);
            using var scope = BeginStandardScope(_log, _nodeId, context.Method);
            using var deadlineScope = RpcDeadlineContext.Push(context.Deadline);
            return await base.UnaryServerHandler(request, context, continuation).ConfigureAwait(false);
        }

        private static Activity? StartServerActivity(string? traceParent, string? traceState, string method)
        {
            ActivityContext parent = default;
            if (!string.IsNullOrEmpty(traceParent))
                _ = ActivityContext.TryParse(traceParent, traceState, out parent);

            return ActivitySourceHolder.StartServer(method, in parent);
        }
    }

    private sealed class NoopDisposable : IDisposable
    {
        internal static readonly NoopDisposable Instance = new();

        void IDisposable.Dispose()
        {
        }
    }

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

        public int Count => _method is null ? 3 : 4;

        public KeyValuePair<string, object?> this[int index] =>
            index switch
            {
                0 => new KeyValuePair<string, object?>("trace_id", FormatTraceId(_activity)),
                1 => new KeyValuePair<string, object?>("span_id", FormatSpanId(_activity)),
                2 => new KeyValuePair<string, object?>("node_id", _nodeId),
                3 when _method is not null => new KeyValuePair<string, object?>("rpc.method", _method),
                _ => throw new ArgumentOutOfRangeException(nameof(index)),
            };

        IEnumerator<KeyValuePair<string, object?>> IEnumerable<KeyValuePair<string, object?>>.GetEnumerator() => new Enumerator(_activity, _nodeId, _method);

        IEnumerator IEnumerable.GetEnumerator() => new Enumerator(_activity, _nodeId, _method);

        private static string FormatSpanId(Activity? activity) => activity is null ? string.Empty : activity.SpanId.ToString();

        private static string FormatTraceId(Activity? activity) => activity is null ? string.Empty : activity.TraceId.ToString();

        /// <summary>Mutable enumerator state lives on a class so ND1903 does not require an immutable struct.</summary>
        private sealed class Enumerator : IEnumerator<KeyValuePair<string, object?>>
        {
            private readonly Activity? _activity;
            private readonly string? _method;
            private readonly string _nodeId;
            private int _index;

            internal Enumerator(Activity? activity, string nodeId, string? method)
            {
                _activity = activity;
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
                        Current = new KeyValuePair<string, object?>("trace_id", FormatTraceId(_activity));
                        return true;
                    case 1:
                        Current = new KeyValuePair<string, object?>("span_id", FormatSpanId(_activity));
                        return true;
                    case 2:
                        Current = new KeyValuePair<string, object?>("node_id", _nodeId);
                        return true;
                    case 3 when _method is not null:
                        Current = new KeyValuePair<string, object?>("rpc.method", _method);
                        return true;
                    default:
                        return false;
                }
            }

            public void Reset() => _index = 0;
        }
    }
}
