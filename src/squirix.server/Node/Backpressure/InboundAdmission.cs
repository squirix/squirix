using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Backpressure;

/// <summary>Shares an inbound RPC's admission with its nested cache pipeline on the same host.</summary>
internal static class InboundAdmission
{
    internal static ValueTask<(Decision Decision, Lease Lease)> AcquireIfNeededAsync(
        IBackpressureGate gate,
        string transport,
        string operation,
        IBackpressureClientIdResolver clients,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        if (gate is AdmissionGate admission && request?.IsAdmitted(admission) == true)
        {
            ObjectDisposedException.ThrowIf(admission.IsDisposed, admission);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult((Decision.Accepted(), Lease.Empty));
        }

        return gate.AcquireAsync(transport, operation, request?.AdmissionClientId ?? clients.Resolve(), cancellationToken);
    }
}
