using System;
using System.Security.Claims;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>
/// Derives backpressure client ids from the JWT subject when authenticated, otherwise from the
/// binary connection id. In-process calls without an <see cref="BinaryRequestContext" /> share the
/// <c language="csharp">runtime</c> bucket. Owner-routed calls from another cluster node resolve to
/// <see cref="ClientIds.InternalOwner" />, which the gate admits only to a free slot, without per-client limits, slowdown or queueing: the entry node already
/// admitted the request under the caller's own client id, and the forwarding connection stands for many callers.
/// </summary>
[Immutable]
internal sealed class BinaryClientIdResolver : IBackpressureClientIdResolver
{
    private const string ConnPrefix = "conn:";
    private const string JwtPrefix = "jwt:";

    private readonly BinaryRequestAccessor _requests;

    internal BinaryClientIdResolver(BinaryRequestAccessor requests)
    {
        ArgumentNullException.ThrowIfNull(requests);
        _requests = requests;
    }

    /// <inheritdoc />
    public string Resolve()
    {
        if (RemoteInvocationContext.IsInternalOwnerInvocation)
            return ClientIds.InternalOwner;

        var context = _requests.Current;
        if (context == null)
            return ClientIds.Runtime;

        return context.ClientId ??= ResolveCore(context);
    }

    private static string CreatePrefixed(string prefix, string value) => string.Concat(prefix, value);

    private static string? GetAuthenticatedPrincipalId(ClaimsPrincipal? user)
    {
        if (user?.Identity?.IsAuthenticated != true)
            return null;

        // JwtBearer maps inbound "sub" to NameIdentifier when MapInboundClaims is enabled (default).
        var subject = user.FindFirst(ClaimTypes.NameIdentifier)?.Value;
        if (string.IsNullOrWhiteSpace(subject))
            subject = user.FindFirst("sub")?.Value;

        return string.IsNullOrWhiteSpace(subject) ? null : subject;
    }

    private static string ResolveCore(BinaryRequestContext context)
    {
        var principalId = GetAuthenticatedPrincipalId(context.User);
        var connectionId = context.ConnectionId ?? context.Connection?.ConnectionId;
        return (principalId, connectionId) switch
        {
            ({ } pid, _) => CreatePrefixed(JwtPrefix, pid),
            (_, { } cid) when !string.IsNullOrWhiteSpace(cid) => CreatePrefixed(ConnPrefix, cid),
            _ => ClientIds.Runtime,
        };
    }
}
