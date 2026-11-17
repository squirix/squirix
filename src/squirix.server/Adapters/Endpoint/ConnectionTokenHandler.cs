using System.Text.Encodings.Web;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Runs the normal JWT pipeline only during the connection handshake.</summary>
internal sealed class ConnectionTokenHandler : JwtBearerHandler
{
    public ConnectionTokenHandler(IOptionsMonitor<JwtBearerOptions> options, ILoggerFactory logger, UrlEncoder encoder)
        : base(options, logger, encoder)
    {
    }

    internal SecurityToken? ValidatedToken { get; private set; }

    protected override async Task InitializeEventsAsync()
    {
        await base.InitializeEventsAsync().ConfigureAwait(false);
        var configured = Events;
        Events = new JwtBearerEvents
        {
            OnMessageReceived = configured.MessageReceived,
            OnAuthenticationFailed = configured.AuthenticationFailed,
            OnChallenge = configured.Challenge,
            OnForbidden = configured.Forbidden,
            OnTokenValidated = async context =>
            {
                ValidatedToken = context.SecurityToken;
                await configured.TokenValidated(context).ConfigureAwait(false);
            },
        };
    }
}
