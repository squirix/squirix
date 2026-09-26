using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Owned capacity and key-stripe lease.</summary>
[ThreadSafe]
internal sealed class ReplicaMutationLease : IDisposable
{
    private readonly AsyncLockHolder _stripe;
    private ReplicaMutationGate? _owner;

    internal ReplicaMutationLease(ReplicaMutationGate owner, AsyncLockHolder stripe)
    {
        _owner = owner;
        _stripe = stripe;
    }

    public void Dispose()
    {
        var owner = Interlocked.Exchange(ref _owner, null);
        owner?.Exit(_stripe);
    }
}
