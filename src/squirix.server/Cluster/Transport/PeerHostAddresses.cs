using System;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Shares an in-flight name lookup for a peer while allowing each RPC to leave at its own deadline.</summary>
internal sealed class PeerHostAddresses
{
    private readonly Func<string, Task<IPAddress[]>> _lookup;
    private readonly Lock _sync = new();
    private Task<IPAddress[]>? _pending;

    internal PeerHostAddresses(DnsEndPoint endpoint, Func<string, Task<IPAddress[]>> lookup)
    {
        Endpoint = endpoint;
        _lookup = lookup;
    }

    internal DnsEndPoint Endpoint { get; }

    internal Task<IPAddress[]> ResolveAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        lock (_sync)
        {
            // A native resolver may keep running after all callers leave. Never queue more lookups behind it.
            if (_pending?.IsCompleted != false)
            {
                _pending = _lookup(Endpoint.Host);
                if (!_pending.IsCompletedSuccessfully)
                {
                    _ = _pending.ContinueWith(
                        static failed => { _ = failed.Exception; },
                        CancellationToken.None,
                        TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                        TaskScheduler.Default);
                }
            }

            return _pending.WaitAsync(cancellationToken);
        }
    }
}
