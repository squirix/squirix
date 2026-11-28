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

    /// <summary>The async dispatch restores its caller's execution context when it returns.</summary>
    /// <returns>A grant revoked when the dispatched operation completes.</returns>
    internal InboundAdmissionScope.Scope EnterDispatchScope() => InboundAdmissionScope.Enter(_gate, false);

    internal ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string operationName, CancellationToken cancellationToken) =>
        _gate.AcquireAsync(CacheOperationNames.CacheTransport, operationName, _clients.Resolve(), cancellationToken);
}
