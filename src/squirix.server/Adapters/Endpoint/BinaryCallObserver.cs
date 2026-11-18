using System;
using Grpc.Core;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Observes admitted binary calls within their inbound correlation and deadline scope.</summary>
internal sealed class BinaryCallObserver
{
    private readonly Action<ServerCallContext> _observe;

    internal BinaryCallObserver(Action<ServerCallContext> observe)
    {
        ArgumentNullException.ThrowIfNull(observe);
        _observe = observe;
    }

    internal void Observe(ServerCallContext context) => _observe(context);
}
