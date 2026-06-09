using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Reliability;

internal interface ICallPolicy : IAsyncDisposable
{
    void BeginDrain();

    ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken);
}
