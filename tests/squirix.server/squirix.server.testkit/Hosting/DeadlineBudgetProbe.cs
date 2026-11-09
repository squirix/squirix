using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Keeps the latest server-side deadline budget observed inside the correlation interceptor for unary
/// cache RPC, so end-to-end tests can assert that the client deadline reaches the node and its forwarded calls.
/// </summary>
public sealed class DeadlineBudgetProbe
{
    private readonly Lock _gate = new();
    private TimeSpan? _lastBudget;

    /// <summary>Gets the latest observed budget, or null when no deadline has been observed on the latest call.</summary>
    public TimeSpan? LastBudget
    {
        get
        {
            lock (_gate)
                return _lastBudget;
        }
    }

    /// <summary>Registers the recording interceptor after the correlation interceptor on the node.</summary>
    /// <param name="services">The node service collection.</param>
    public void Register(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IConfigureOptions<GrpcServiceOptions>>(new ProbeOptionsConfigurator(this)));
    }

    private void Record(TimeSpan? budget)
    {
        lock (_gate)
            _lastBudget = budget;
    }

    [Immutable]
    internal sealed class ProbeInterceptor : Interceptor
    {
        private const string CacheServiceSegment = "SquirixCacheService/";

        private readonly DeadlineBudgetProbe _probe;

        public ProbeInterceptor(DeadlineBudgetProbe probe)
        {
            _probe = probe;
        }

        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            if (context.Method.Contains(CacheServiceSegment, StringComparison.Ordinal))
                _probe.Record(ServerRpcDeadlineContext.GetRemainingBudget());

            return continuation(request, context);
        }
    }

    [Immutable]
    private sealed class ProbeOptionsConfigurator : IConfigureOptions<GrpcServiceOptions>
    {
        private readonly DeadlineBudgetProbe _probe;

        internal ProbeOptionsConfigurator(DeadlineBudgetProbe probe)
        {
            _probe = probe;
        }

        public void Configure(GrpcServiceOptions options) => options.Interceptors.Add<ProbeInterceptor>(_probe);
    }
}
