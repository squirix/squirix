using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit;

/// <summary>Symmetric JWT credentials shared by a test node and its callers.</summary>
[Immutable]
public sealed class TestJwtCredentials
{
    private readonly byte[] _signingKey;

    /// <summary>Initializes a new instance of the <see cref="TestJwtCredentials" /> class.</summary>
    /// <param name="signingKey">Symmetric HMAC signing key bytes.</param>
    /// <param name="issuer">JWT issuer claim value.</param>
    /// <param name="audience">JWT audience claim value.</param>
    public TestJwtCredentials(byte[] signingKey, string issuer, string audience)
        : this(signingKey.AsSpan(), issuer, audience)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="TestJwtCredentials" /> class.</summary>
    /// <param name="signingKey">Symmetric HMAC signing key bytes.</param>
    /// <param name="issuer">JWT issuer claim value.</param>
    /// <param name="audience">JWT audience claim value.</param>
    private TestJwtCredentials(ReadOnlySpan<byte> signingKey, string issuer, string audience)
    {
        _signingKey = new byte[signingKey.Length];
        signingKey.CopyTo(_signingKey);
        Base64SigningKey = Convert.ToBase64String(_signingKey);
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
    public byte[] GetSigningKey() => FixtureBufferKit.CopyToOwned(_signingKey);
}
