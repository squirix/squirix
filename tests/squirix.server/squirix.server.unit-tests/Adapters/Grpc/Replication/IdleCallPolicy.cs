using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>A call policy no call goes through; replication calls lease the channel directly.</summary>
[Immutable]
internal sealed class IdleCallPolicy : IServerCallPolicy
{
    public void BeginDrain()
    {
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    public ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken) =>
        throw new NotSupportedException("IdleCallPolicy does not execute calls.");
}
