using System;
using System.Security.Claims;
using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Squirix.Server.Node.Hosting.Security;

internal sealed class ApiKeyAuthenticationHandler : AuthenticationHandler<AuthenticationSchemeOptions>
{
    private readonly ApiKeyAuthSettings _settings;

    public ApiKeyAuthenticationHandler(IOptionsMonitor<AuthenticationSchemeOptions> options, ILoggerFactory logger, UrlEncoder encoder, ApiKeyAuthSettings settings)
        : base(options, logger, encoder)
    {
        _settings = settings;
    }

    protected override Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        if (!_settings.IsEnabled)
            return Task.FromResult(AuthenticateResult.NoResult());

        string? apiKey = null;

        if (Request.Headers.TryGetValue(ApiKeyAuthenticationDefaults.HeaderName, out var values) && values.Count > 0)
            apiKey = values[0];

        if (string.IsNullOrEmpty(apiKey) && Request.Headers.TryGetValue("Authorization", out var authValues))
        {
            var auth = authValues.Count > 0 ? authValues[0] : null;
            if (!string.IsNullOrWhiteSpace(auth))
            {
                if (auth.StartsWith("Bearer ", StringComparison.OrdinalIgnoreCase))
                    apiKey = auth["Bearer ".Length..].Trim();
                else if (auth.StartsWith("ApiKey ", StringComparison.OrdinalIgnoreCase))
                    apiKey = auth["ApiKey ".Length..].Trim();
            }
        }

        if (!_settings.IsAllowed(apiKey))
            return Task.FromResult(AuthenticateResult.Fail("Invalid or missing API key."));

        var claims = new[] { new Claim(ClaimTypes.Name, "api-key-user") };
        var identity = new ClaimsIdentity(claims, ApiKeyAuthenticationDefaults.Scheme);
        var principal = new ClaimsPrincipal(identity);
        var ticket = new AuthenticationTicket(principal, ApiKeyAuthenticationDefaults.Scheme);

        return Task.FromResult(AuthenticateResult.Success(ticket));
    }
}
