using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class CacheRequestAdmission
{
    private readonly IBackpressureClientIdResolver _clients;
    private readonly IBackpressureGate _gate;

    public CacheRequestAdmission(IBackpressureGate gate, IBackpressureClientIdResolver clients)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(clients);
        _gate = gate;
        _clients = clients;
    }

    internal InboundAdmissionScope Enter() => InboundAdmissionScope.Enter(_gate);

    internal ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string method, CancellationToken cancellationToken) =>
        _gate.AcquireAsync(CacheOperationNames.CacheTransport, Operation(method), _clients.Resolve(), cancellationToken);

    private static string Operation(string method) => method.AsSpan(method.AsSpan().LastIndexOf('/') + 1) switch
    {
        "GetEntry" or "GetExpiration" => CacheOperationNames.GetEntry,
        "GetValue" or "GetDistributed" => CacheOperationNames.Get,
        "SetEntry" or "SetDistributed" => CacheOperationNames.Set,
        "TryAddEntry" => CacheOperationNames.TryAdd,
        "GetOrAdd" => CacheOperationNames.GetOrAdd,
        "Remove" => CacheOperationNames.Remove,
        "RemoveExpiration" => CacheOperationNames.RemoveExpiration,
        "Touch" or "RefreshDistributed" => CacheOperationNames.Touch,
        "Update" => CacheOperationNames.Update,
        _ => throw new InvalidOperationException("Unsupported cache RPC admission operation."),
    };
}
