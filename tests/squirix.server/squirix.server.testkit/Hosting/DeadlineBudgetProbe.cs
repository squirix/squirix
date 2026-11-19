using System;
using System.Threading;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
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
        _ = services.AddSingleton(new BinaryCallObserver(_ => Record(ServerRpcDeadlineContext.CurrentBudget?.Remaining)));
    }

    private void Record(TimeSpan? budget)
    {
        lock (_gate)
            _lastBudget = budget;
    }
}
