using System;
using System.Diagnostics;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Membership;

namespace Squirix.Server.Node.Observability;

/// <summary>
/// Utilities and interceptors for structured logging scopes and trace-context propagation.
/// </summary>
internal static class Correlation
{
    private const string TraceParentHeader = "traceparent";
    private const string TraceStateHeader = "tracestate";

    internal static IDisposable BeginStandardScope(ILogger logger, string nodeId, string? method = null)
    {
        var act = Activity.Current;
        var traceId = act?.TraceId.ToString() ?? string.Empty;
        var spanId = act?.SpanId.ToString() ?? string.Empty;
        var scope = logger.BeginScope(new StandardScopeState(traceId, spanId, nodeId, method));
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
            var callOptions = AttachTraceHeaders(context.Options, context.Method.FullName);
            var ctx2 = new ClientInterceptorContext<TRequest, TResponse>(context.Method, context.Host, callOptions);
            using var scope = BeginStandardScope(_log, _nodeId, context.Method.FullName);
            return base.AsyncUnaryCall(request, ctx2, continuation);
        }

        private static CallOptions AttachTraceHeaders(CallOptions opt, string method)
        {
            var meta = opt.Headers ?? [];

            // Ensure there is an Activity to propagate; keep it cheap.
            var activity = Activity.Current ?? ActivitySourceHolder.StartClient(method);

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
        public static readonly NoopDisposable Instance = new();

        public void Dispose()
        {
        }
    }
}
