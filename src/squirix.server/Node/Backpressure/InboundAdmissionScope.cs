using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

/// <summary>Shares an inbound RPC's admission with its nested cache pipeline on the same host.</summary>
[ThreadSafe]
internal sealed class InboundAdmissionScope
{
    private static readonly AsyncLocal<InboundAdmissionScope?> Current = new();
    private IBackpressureGate? _gate;

    private InboundAdmissionScope(IBackpressureGate gate)
    {
        _gate = gate;
    }

    internal static ValueTask<(Decision Decision, Lease Lease)> AcquireIfNeededAsync(
        IBackpressureGate gate,
        string transport,
        string operation,
        IBackpressureClientIdResolver clients,
        CancellationToken cancellationToken)
    {
        if (gate is AdmissionGate admission && IsAdmitted(admission))
        {
            ObjectDisposedException.ThrowIf(admission.IsDisposed, admission);
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult((Decision.Accepted(), Lease.Empty));
        }

        return gate.AcquireAsync(transport, operation, clients.Resolve(), cancellationToken);
    }

    internal static Scope Enter(IBackpressureGate gate, bool restoreOnDispose = true)
    {
        var prior = restoreOnDispose ? Current.Value : null;
        var grant = new InboundAdmissionScope(gate);
        Current.Value = grant;
        return new Scope(grant, prior, restoreOnDispose);
    }

    internal static bool IsAdmitted(IBackpressureGate gate) => Current.Value is { } scope && ReferenceEquals(Volatile.Read(ref scope._gate), gate);

    internal readonly struct Scope : IDisposable
    {
        private readonly InboundAdmissionScope _grant;
        private readonly InboundAdmissionScope? _prior;
        private readonly bool _restoreOnDispose;

        internal Scope(InboundAdmissionScope grant, InboundAdmissionScope? prior, bool restoreOnDispose)
        {
            _grant = grant;
            _prior = prior;
            _restoreOnDispose = restoreOnDispose;
        }

        public void Dispose()
        {
            // Revoke every inherited copy before the admission lease is returned.
            _ = Interlocked.Exchange(ref _grant._gate, null);
            if (_restoreOnDispose)
                Current.Value = _prior;
        }
    }
}
