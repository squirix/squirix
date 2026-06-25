using System;

namespace Squirix.Server.Node.Backpressure;

internal readonly struct BackpressureLease : IDisposable
{
    private readonly BackpressureGate.ClientState? _client;
    private readonly string? _clientId;
    private readonly BackpressureGate? _gate;

    internal BackpressureLease(BackpressureGate gate, string clientId, BackpressureGate.ClientState client)
    {
        _gate = gate;
        _clientId = clientId;
        _client = client;
    }

    public static BackpressureLease Empty => default;

    public void Dispose() => _gate?.ReleaseLease(_clientId!, _client!);
}
