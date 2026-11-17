using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Squirix.Transport.Grpc;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Authenticates requests only from the identity established by their TLS connection.</summary>
internal sealed class ConnectionAuthenticationHandler : AuthenticationHandler<JwtBearerOptions>
{
    private readonly ConnectionAuthenticationValidator _validator;
    private bool _renewalRequired;

    public ConnectionAuthenticationHandler(
        IOptionsMonitor<JwtBearerOptions> options,
        ILoggerFactory logger,
        UrlEncoder encoder,
        ConnectionAuthenticationValidator validator)
        : base(options, logger, encoder)
    {
        _validator = validator;
    }

    protected override async Task<AuthenticateResult> HandleAuthenticateAsync()
    {
        var connection = Context.Features.Get<AuthenticatedConnection>();
        if (connection == null || Request.Headers.ContainsKey("Authorization"))
            return AuthenticateResult.Fail("The request has no authenticated connection identity.");
        if (!await _validator.IsCurrentAsync(connection, Context.RequestAborted).ConfigureAwait(false))
        {
            _renewalRequired = true;
            connection.RequestClose();
            return AuthenticateResult.Fail("The connection must authenticate again.");
        }

        var principal = connection.ClonePrincipal();
        return principal == null
            ? AuthenticateResult.Fail("The connection identity cannot provide an independent authorization snapshot.")
            : AuthenticateResult.Success(new AuthenticationTicket(principal, Scheme.Name));
    }

    protected override Task HandleChallengeAsync(AuthenticationProperties properties)
    {
        if (!_renewalRequired || Request.ContentType?.StartsWith("application/grpc", System.StringComparison.OrdinalIgnoreCase) != true)
        {
            Response.StatusCode = StatusCodes.Status401Unauthorized;
            return Task.CompletedTask;
        }

        Response.StatusCode = StatusCodes.Status200OK;
        Response.ContentType = "application/grpc";
        Response.Headers["grpc-status"] = "14";
        Response.Headers["grpc-message"] = ConnectionAuthenticationProtocol.RenewalRequired;
        return Task.CompletedTask;
    }
}
