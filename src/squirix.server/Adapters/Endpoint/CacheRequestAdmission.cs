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

    internal CacheRequestAdmission(IBackpressureGate gate, IBackpressureClientIdResolver clients)
    {
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(clients);
        _gate = gate;
        _clients = clients;
    }

    internal void Grant(BinaryRequestContext call) => call.SetAdmissionGate(_gate);

    internal ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string operationName, BinaryRequestContext call, CancellationToken cancellationToken)
    {
        var clientId = ResolveClientId(call);
        call.AdmissionClientId = clientId;
        return _gate.AcquireAsync(CacheOperationNames.CacheTransport, operationName, clientId, cancellationToken);
    }

    internal string ResolveClientId(BinaryRequestContext call) => (call.Forwarded, _clients) switch
    {
        (true, _) => ClientIds.InternalOwner,
        (_, BinaryClientIdResolver) => BinaryClientIdResolver.ResolveRequest(call),
        _ => _clients.Resolve(),
    };
}
