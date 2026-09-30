using System;
using System.Collections.Concurrent;
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
/// Records the server-side remaining deadline budget observed inside the correlation interceptor for every unary
/// cache RPC, so end-to-end tests can assert that the client deadline reaches the node and its forwarded calls.
/// </summary>
public sealed class DeadlineBudgetProbe
{
    private readonly ConcurrentQueue<TimeSpan?> _budgets = new();

    /// <summary>Gets the recorded budgets in arrival order; a <see langword="null" /> entry means the call carried no deadline.</summary>
    /// <returns>A snapshot of the recorded budgets.</returns>
    public TimeSpan?[] Snapshot() => [.. _budgets];

    /// <summary>Registers the recording interceptor after the correlation interceptor on the node.</summary>
    /// <param name="services">The node service collection.</param>
    public void Register(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IConfigureOptions<GrpcServiceOptions>>(new ProbeOptionsConfigurator(this)));
    }

    internal void Record(TimeSpan? budget) => _budgets.Enqueue(budget);

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
                _probe.Record(ServerRpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow));

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
