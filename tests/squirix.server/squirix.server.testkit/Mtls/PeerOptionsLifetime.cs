using System;
using System.Threading;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Keeps the certificate material for one test node startup's outbound socket options alive.</summary>
[Mutable]
internal sealed class PeerOptionsLifetime : IDisposable
{
    private readonly Lock _gate = new();
    private MtlsCertificate.Hold? _hold;
    private int _disposed;

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        lock (_gate)
        {
            _hold?.Dispose();
            _hold = null;
        }
    }

    internal void Hold(MtlsCertificate.Hold hold)
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
            {
                hold.Dispose();
                return;
            }

            _hold = hold;
        }
    }

    internal void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
