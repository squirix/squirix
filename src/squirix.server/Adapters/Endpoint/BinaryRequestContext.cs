using System;
using System.Security.Claims;
using System.Threading;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Metadata carried from the listener into authorization and cache dispatch.</summary>
internal sealed class BinaryRequestContext
{
    internal required string Path { get; init; }

    internal required IServiceProvider RequestServices { get; init; }

    internal string? ConnectionId { get; init; }

    internal BinaryConnection? Connection { get; init; }

    internal ClaimsPrincipal? User { get; set; }

    internal CancellationToken RequestAborted { get; init; }

    internal string? TraceParent { get; set; }

    internal string? TraceState { get; set; }

    internal string? ClientId { get; set; }
}

/// <summary>Supplies the active remote request to the admission gate within an asynchronous operation.</summary>
internal sealed class BinaryRequestAccessor
{
    private readonly AsyncLocal<Holder?> _active = new();

    internal BinaryRequestContext? Current
    {
        get => _active.Value?.Context;
        set
        {
            if (_active.Value is { } previous)
                previous.Context = null;
            _active.Value = value == null ? null : new Holder { Context = value };
        }
    }

    private sealed class Holder
    {
        internal BinaryRequestContext? Context { get; set; }
    }
}
