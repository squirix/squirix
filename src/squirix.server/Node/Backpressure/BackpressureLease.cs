using System;

namespace Squirix.Server.Node.Backpressure;

internal readonly struct BackpressureLease : IDisposable
{
    private readonly BackpressureGate? _gate;
    private readonly string? _clientId;
    private readonly BackpressureGate.ClientState? _client;

    internal BackpressureLease(BackpressureGate gate, string clientId, BackpressureGate.ClientState client)
    {
        _gate = gate;
        _clientId = clientId;
        _client = client;
    }

    public static BackpressureLease Empty => default;

    public void Dispose()
    {
        if (_gate is not null)
            _gate.ReleaseLease(_clientId!, _client!);
    }
}
