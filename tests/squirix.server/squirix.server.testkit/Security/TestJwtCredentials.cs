using System;

namespace Squirix.Server.TestKit.Security;

/// <summary>Symmetric JWT credentials shared by a test node and its callers.</summary>
public sealed class TestJwtCredentials
{
    private readonly byte[] _signingKey;

    /// <summary>Initializes a new instance of the <see cref="TestJwtCredentials" /> class.</summary>
    /// <param name="signingKey">Symmetric HMAC signing key bytes.</param>
    /// <param name="issuer">JWT issuer claim value.</param>
    /// <param name="audience">JWT audience claim value.</param>
    public TestJwtCredentials(byte[] signingKey, string issuer, string audience)
    {
        ArgumentNullException.ThrowIfNull(signingKey);

        _signingKey = CopySigningKey(signingKey);
        Base64SigningKey = Convert.ToBase64String(signingKey);
        Issuer = issuer;
        Audience = audience;
    }

    /// <summary>Gets the JWT audience claim value.</summary>
    public string Audience { get; }

    /// <summary>Gets the base64-encoded signing key.</summary>
    public string Base64SigningKey { get; }

    /// <summary>Gets the JWT issuer claim value.</summary>
    public string Issuer { get; }

    /// <summary>Gets an independent copy of the raw symmetric signing key bytes.</summary>
    /// <returns>Raw symmetric signing key bytes.</returns>
    public byte[] GetSigningKey() => CopySigningKey(_signingKey);

    private static byte[] CopySigningKey(byte[] signingKey)
    {
        var copy = new byte[signingKey.Length];
        Array.Copy(signingKey, copy, signingKey.Length);
        return copy;
    }
}
