using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

[Immutable]
internal sealed record Lease : IDisposable
{
    private readonly string? _clientId;
    private readonly AdmissionGate? _gate;

    internal Lease(AdmissionGate gate, string clientId)
    {
        _gate = gate;
        _clientId = clientId;
    }

    private Lease()
    {
    }

    internal static Lease Empty { get; } = new();

    public void Dispose() => _gate?.ReleaseLease(_clientId!);
}
