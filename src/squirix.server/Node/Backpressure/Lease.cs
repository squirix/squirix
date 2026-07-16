using System;

namespace Squirix.Server.Node.Backpressure;

internal sealed record Lease : IDisposable
{
    private readonly AdmissionGate.ClientState? _client;
    private readonly string? _clientId;
    private readonly AdmissionGate? _gate;

    public Lease(AdmissionGate gate, string clientId, AdmissionGate.ClientState client)
    {
        _gate = gate;
        _clientId = clientId;
        _client = client;
    }

    private Lease()
    {
    }

    internal static Lease Empty { get; } = new();

    public void Dispose() => _gate?.ReleaseLease(_clientId!, _client!);
}
