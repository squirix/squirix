using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Backpressure;

[ThreadSafe]
internal sealed class Lease : IDisposable
{
    private readonly AdmissionGate.ClientState? _client;
    private readonly string? _clientId;
    private readonly AdmissionGate? _gate;
    private int _released;

    internal Lease(AdmissionGate gate, string clientId, AdmissionGate.ClientState client)
    {
        _gate = gate;
        _clientId = clientId;
        _client = client;
    }

    private Lease()
    {
    }

    internal static Lease Empty { get; } = new();

    public void Dispose()
    {
        if (_gate == null || Interlocked.Exchange(ref _released, 1) != 0)
            return;

        _gate.ReleaseLease(_clientId!, _client!);
    }
}
