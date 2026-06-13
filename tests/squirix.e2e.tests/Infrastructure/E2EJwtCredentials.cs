namespace Squirix.E2ETests.Infrastructure;

internal sealed class E2EJwtCredentials
{
    public E2EJwtCredentials(byte[] signingKey, string base64SigningKey, string issuer, string audience)
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
