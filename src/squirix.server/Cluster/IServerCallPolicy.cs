using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Transport;

internal interface IServerCallPolicy : IAsyncDisposable
{
    void BeginDrain();

    ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken);
}
