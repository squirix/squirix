namespace Squirix.E2ETests.Support.Auth;

internal sealed class JwtCredentials
{
    internal JwtCredentials(byte[] signingKey, string base64SigningKey, string issuer, string audience)
    {
        SigningKey = signingKey;
        Base64SigningKey = base64SigningKey;
        Issuer = issuer;
        Audience = audience;
    }

    public string Audience { get; }

    public string Base64SigningKey { get; }

    public string Issuer { get; }

    public byte[] SigningKey { get; }
}
