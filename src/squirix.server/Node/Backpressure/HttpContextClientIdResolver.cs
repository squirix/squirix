using System;
using System.Security.Claims;
using Microsoft.AspNetCore.Http;

namespace Squirix.Server.Node.Backpressure;

/// <summary>
/// Derives backpressure client ids from the JWT subject when authenticated, otherwise from the
/// ASP.NET Core connection id. In-process calls without an <see cref="HttpContext" /> share the
/// <c>runtime</c> bucket.
/// </summary>
internal sealed class HttpContextClientIdResolver : IBackpressureClientIdResolver
{
    internal const string MissingHttpContextClientId = "runtime";

    private const string CachedClientIdItemKey = "__squirix.backpressure.client_id";
    private const string ConnPrefix = "conn:";
    private const string JwtPrefix = "jwt:";

    private readonly IHttpContextAccessor _httpContextAccessor;

    internal HttpContextClientIdResolver(IHttpContextAccessor httpContextAccessor)
    {
        _httpContextAccessor = httpContextAccessor ?? throw new ArgumentNullException(nameof(httpContextAccessor));
    }

    /// <inheritdoc />
    public string Resolve()
    {
        var context = _httpContextAccessor.HttpContext;
        if (context is null)
            return MissingHttpContextClientId;

        if (context.Items.TryGetValue(CachedClientIdItemKey, out var cached) && cached is string clientId)
            return clientId;

        clientId = ResolveCore(context);
        context.Items[CachedClientIdItemKey] = clientId;
        return clientId;
    }

    private static string CreatePrefixed(string prefix, string value) => string.Create(
        prefix.Length + value.Length,
        (prefix, value),
        static (span, state) =>
        {
            state.prefix.AsSpan().CopyTo(span);
            state.value.AsSpan().CopyTo(span[state.prefix.Length..]);
        });

    private static string ResolveCore(HttpContext context)
    {
        var principalId = GetAuthenticatedPrincipalId(context.User);
        if (principalId is not null)
            return CreatePrefixed(JwtPrefix, principalId);

        var connectionId = context.Connection.Id;
        if (!string.IsNullOrWhiteSpace(connectionId))
            return CreatePrefixed(ConnPrefix, connectionId);

        return MissingHttpContextClientId;
    }

    private static string? GetAuthenticatedPrincipalId(ClaimsPrincipal? user)
    {
        if (user?.Identity?.IsAuthenticated is not true)
            return null;

        // JwtBearer maps inbound "sub" to NameIdentifier when MapInboundClaims is enabled (default).
        var subject = user.FindFirstValue(ClaimTypes.NameIdentifier);
        if (string.IsNullOrWhiteSpace(subject))
            subject = user.FindFirstValue("sub");

        return string.IsNullOrWhiteSpace(subject) ? null : subject;
    }
}
