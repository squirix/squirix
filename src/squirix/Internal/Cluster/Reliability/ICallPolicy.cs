using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Observability;

namespace Squirix.Internal.Cluster.Reliability;

internal interface ICallPolicy : IAsyncDisposable
{
    void BeginDrain();

    ValueTask<T> ExecuteAsync<TState, T>(Func<TState, CancellationToken, ValueTask<T>> action, TState state, CancellationToken cancellationToken, DeadlineBudget? budget = null);
}
