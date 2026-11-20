using System;
using System.Security.Claims;
using System.Threading;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Metadata carried from the listener into authorization and cache dispatch.</summary>
internal sealed class BinaryRequestContext
{
    internal required string Path { get; init; }

    internal IServiceProvider? RequestServices { get; init; }

    internal string? ConnectionId { get; init; }

    internal BinaryConnection? Connection { get; init; }

    internal ClaimsPrincipal? User { get; set; }

    internal CancellationToken RequestAborted { get; init; }

    internal string? TraceParent { get; set; }

    internal string? TraceState { get; set; }

    internal string? ClientId { get; set; }
}
