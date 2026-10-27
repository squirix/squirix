using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

/// <summary>
/// The slot an admitted request holds. It must be disposed exactly once; the gate throws on a release with no outstanding lease,
/// so a lease value (or a copy of it) disposed twice is caught as long as no other lease is outstanding to absorb it.
/// </summary>
[ThreadSafe]
internal readonly record struct Lease : IDisposable
{
    private readonly AdmissionGate.ClientState? _client;
    private readonly string? _clientId;
    private readonly AdmissionGate? _gate;

    internal Lease(AdmissionGate gate, string clientId, AdmissionGate.ClientState client)
    {
        _gate = gate;
        _clientId = clientId;
        _client = client;
    }

    internal static Lease Empty => default;

    public void Dispose() => _gate?.ReleaseLease(_clientId!, _client!);
}
