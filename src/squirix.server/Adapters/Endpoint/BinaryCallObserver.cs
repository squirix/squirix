using System;
using Microsoft.AspNetCore.Http;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Observes admitted binary calls within their inbound correlation and deadline scope.</summary>
internal sealed class BinaryCallObserver
{
    private readonly Action<HttpContext> _observe;

    internal BinaryCallObserver(Action<HttpContext> observe)
    {
        ArgumentNullException.ThrowIfNull(observe);
        _observe = observe;
    }

    internal void Observe(HttpContext context) => _observe(context);
}
