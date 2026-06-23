using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Reliability;

internal interface ICallPolicy : IAsyncDisposable
{
    void BeginDrain();

    ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken);
}
