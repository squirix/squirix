using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Observability;
using Squirix.Transport;

namespace Squirix.Internal.Cluster.Reliability;

internal interface ICallPolicy : IAsyncDisposable
{
    void BeginDrain();

    ValueTask<T> ExecuteTransportAsync<TState, T>(Func<TState, CancellationToken, long, ValueTask<T>> action, TState state, DeadlineBudget budget, Action<SquirixTransportException>? onFailure, CancellationToken cancellationToken);

    ValueTask<T> ExecuteAsync<TState, T>(Func<TState, CancellationToken, ValueTask<T>> action, TState state, CancellationToken cancellationToken, DeadlineBudget? budget = null, Action<SquirixTransportException>? onFailure = null);
}
