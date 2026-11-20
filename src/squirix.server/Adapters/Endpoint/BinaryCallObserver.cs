using System;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Observes admitted binary calls within their inbound correlation and deadline scope.</summary>
internal sealed class BinaryCallObserver
{
    private readonly Action<BinaryRequestContext> _observe;

    internal BinaryCallObserver(Action<BinaryRequestContext> observe)
    {
        ArgumentNullException.ThrowIfNull(observe);
        _observe = observe;
    }

    internal void Observe(BinaryRequestContext context) => _observe(context);
}
