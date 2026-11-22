using System.Collections.Generic;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class JwtMetadata
{
    internal string? Issuer { get; init; }

    internal List<JwtVerificationKey> SigningKeys { get; } = [];
}
