using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

/// <summary>Shares an inbound RPC's admission with its nested cache pipeline on the same host.</summary>
[ThreadSafe]
internal sealed class InboundAdmissionScope : IDisposable
{
    private static readonly AsyncLocal<InboundAdmissionScope?> Current = new();
    private readonly InboundAdmissionScope? _previous;
    private IBackpressureGate? _gate;

    private InboundAdmissionScope(IBackpressureGate gate)
    {
        _gate = gate;
        _previous = Current.Value;
        Current.Value = this;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        // Invalidate inherited execution contexts before the owning admission lease is released.
        _ = Interlocked.Exchange(ref _gate, null);
        Current.Value = _previous;
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

    internal static InboundAdmissionScope Enter(IBackpressureGate gate) => new(gate);

    internal static bool IsAdmitted(IBackpressureGate gate) => Current.Value is { } scope && ReferenceEquals(Volatile.Read(ref scope._gate), gate);
}
