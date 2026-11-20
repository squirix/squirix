using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class JwtValidationOptions
{
    internal TokenValidationParameters TokenValidationParameters { get; set; } = new();

    internal IConfigurationManager<OpenIdConnectConfiguration>? ConfigurationManager { get; set; }

    internal bool RequireHttpsMetadata { get; set; }
}
