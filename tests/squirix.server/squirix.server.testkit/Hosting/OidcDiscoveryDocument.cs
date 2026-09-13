using System.Text.Json.Serialization;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>OIDC discovery document for mock authority.</summary>
[Immutable]
internal sealed class OidcDiscoveryDocument
{
    [JsonPropertyName("issuer")]
    [JsonInclude]
    internal required string Issuer { get; init; }

    [JsonPropertyName("jwks_uri")]
    [JsonInclude]
    internal required string JwksEndpoint { get; init; }
}
