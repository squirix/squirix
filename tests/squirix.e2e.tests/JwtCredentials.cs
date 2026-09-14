using Squirix.Attributes;

namespace Squirix.E2ETests;

[Immutable]
internal sealed class JwtCredentials
{
    internal JwtCredentials(byte[] signingKey, string base64SigningKey, string issuer, string audience)
    {
        SigningKey = signingKey;
        Base64SigningKey = base64SigningKey;
        Issuer = issuer;
        Audience = audience;
    }

    internal string Audience { get; }

    internal string Base64SigningKey { get; }

    internal string Issuer { get; }

    internal byte[] SigningKey { get; }
}
