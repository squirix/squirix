using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class JwtVerificationKey
{
    private readonly byte[] _material;
    private readonly string _family;
    private readonly string? _algorithm;
    private readonly int _bits;

    internal JwtVerificationKey(byte[] secret)
        : this(secret, "HS", checked(secret.Length * 8), null)
    {
    }

    private JwtVerificationKey(byte[] material, string family, int bits, string? algorithm)
    {
        _material = material;
        _family = family;
        _bits = bits;
        _algorithm = algorithm;
    }

    internal string? KeyId { get; init; }

    internal string? Thumbprint { get; private set; }

    internal DateTime? NotBefore { get; private set; }

    internal DateTime? NotAfter { get; private set; }

    internal static JwtVerificationKey? FromJwk(JsonElement jwk)
    {
        try
        {
            return ReadAuthorityKey(jwk);
        }
        catch (Exception ex) when (ex is FormatException or ArgumentException or CryptographicException or InvalidOperationException or KeyNotFoundException)
        {
            return null;
        }
    }

    internal bool Matches(string? kid, string? thumbprint) =>
        (kid != null && string.Equals(KeyId, kid, Thumbprint == null ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase)) ||
        (thumbprint != null && (string.Equals(KeyId, thumbprint, Thumbprint == null ? StringComparison.Ordinal : StringComparison.OrdinalIgnoreCase) ||
                                string.Equals(Thumbprint, thumbprint, StringComparison.OrdinalIgnoreCase)));

    internal bool Verify(ReadOnlySpan<byte> input, ReadOnlySpan<byte> signature, string algorithm)
    {
        if (_algorithm != null && !string.Equals(_algorithm, algorithm, StringComparison.Ordinal))
            return false;
        var bits = DigestSize(algorithm);
        var hash = bits switch
        {
            256 => HashAlgorithmName.SHA256,
            384 => HashAlgorithmName.SHA384,
            512 => HashAlgorithmName.SHA512,
            _ => default,
        };
        if (bits == 0)
            return false;

        try
        {
            return _family switch
            {
                "HS" when algorithm[0] == 'H' && _bits >= bits && signature.Length == bits / 8 => VerifyMac(input, signature, hash),
                "RSA" when algorithm[0] is 'R' or 'P' && _bits >= 1024 => VerifyRsa(input, signature, hash, algorithm[0]),
                "EC" when algorithm[0] == 'E' && _bits == (bits == 512 ? 521 : bits) => VerifyEc(input, signature, hash),
                _ => false,
            };
        }
        catch (Exception ex) when (ex is CryptographicException or ArgumentException)
        {
            return false;
        }
    }

    private static JwtVerificationKey? ReadAuthorityKey(JsonElement jwk)
    {
        if (!SupportsVerification(jwk))
            return null;
        var id = Read(jwk, "kid");
        var alg = Read(jwk, "alg");
        var kind = Read(jwk, "kty");
        if (kind is not ("RSA" or "EC"))
            return null;
        if (jwk.TryGetProperty("x5c", out var chain) && chain.GetArrayLength() > 0)
            return FromCertificate(jwk, chain[0], kind, id, alg);

        if (string.Equals(kind, "RSA", StringComparison.Ordinal))
        {
            using var rsa = RSA.Create(new RSAParameters { Modulus = Decode(jwk, "n"), Exponent = Decode(jwk, "e") });
            return new JwtVerificationKey(rsa.ExportSubjectPublicKeyInfo(), kind, rsa.KeySize, alg) { KeyId = id };
        }

        var curve = Read(jwk, "crv") switch
        {
            "P-256" => ECCurve.NamedCurves.nistP256,
            "P-384" => ECCurve.NamedCurves.nistP384,
            "P-521" => ECCurve.NamedCurves.nistP521,
            _ => default,
        };
        if (!curve.IsNamed)
            return null;
        using var ec = ECDsa.Create(new ECParameters { Curve = curve, Q = new ECPoint { X = Decode(jwk, "x"), Y = Decode(jwk, "y") } });
        return new JwtVerificationKey(ec.ExportSubjectPublicKeyInfo(), kind, ec.KeySize, alg) { KeyId = id };
    }

    private static JwtVerificationKey? FromCertificate(JsonElement jwk, JsonElement leaf, string kind, string? id, string? algorithm)
    {
        using var certificate = X509CertificateLoader.LoadCertificate(Convert.FromBase64String(leaf.GetString()!));
        using AsymmetricAlgorithm? publicKey = string.Equals(kind, "RSA", StringComparison.Ordinal) ? certificate.GetRSAPublicKey() : certificate.GetECDsaPublicKey();
        var thumbprint = Base64Url.EncodeToString(certificate.GetCertHash());
        var supplied = Read(jwk, "x5t");
        return publicKey != null && MatchesCertificateParameters(jwk, publicKey) && (supplied == null || string.Equals(thumbprint, supplied, StringComparison.Ordinal))
            ? new JwtVerificationKey(publicKey.ExportSubjectPublicKeyInfo(), kind, publicKey.KeySize, algorithm)
            {
                KeyId = id,
                Thumbprint = thumbprint,
                NotBefore = certificate.NotBefore.ToUniversalTime(),
                NotAfter = certificate.NotAfter.ToUniversalTime(),
            }
            : null;
    }

    private static bool MatchesCertificateParameters(JsonElement jwk, AsymmetricAlgorithm key)
    {
        if (key is RSA rsa && (jwk.TryGetProperty("n", out _) || jwk.TryGetProperty("e", out _)))
        {
            var parameters = rsa.ExportParameters(false);
            return Decode(jwk, "n").AsSpan().SequenceEqual(parameters.Modulus) && Decode(jwk, "e").AsSpan().SequenceEqual(parameters.Exponent);
        }

        if (key is ECDsa ec)
        {
            var parameters = ec.ExportParameters(false);
            var (curveName, curve) = ec.KeySize switch
            {
                256 => (Jwk: "P-256", Curve: ECCurve.NamedCurves.nistP256),
                384 => (Jwk: "P-384", Curve: ECCurve.NamedCurves.nistP384),
                521 => (Jwk: "P-521", Curve: ECCurve.NamedCurves.nistP521),
                _ => default,
            };
            if (curveName == null || !string.Equals(curve.Oid.Value, parameters.Curve.Oid.Value, StringComparison.Ordinal))
                return false;
            if (jwk.TryGetProperty("x", out _) || jwk.TryGetProperty("y", out _) || jwk.TryGetProperty("crv", out _))
            {
                return string.Equals(Read(jwk, "crv"), curveName, StringComparison.Ordinal) &&
                       Decode(jwk, "x").AsSpan().SequenceEqual(parameters.Q.X) && Decode(jwk, "y").AsSpan().SequenceEqual(parameters.Q.Y);
            }
        }

        return true;
    }

    private static int DigestSize(string algorithm) => algorithm switch
    {
        "HS256" or "RS256" or "PS256" or "ES256" => 256,
        "HS384" or "RS384" or "PS384" or "ES384" => 384,
        "HS512" or "RS512" or "PS512" or "ES512" => 512,
        _ => 0,
    };

    private static bool SupportsVerification(JsonElement jwk)
    {
        var use = Read(jwk, "use");
        if (use != null && !string.Equals(use, "sig", StringComparison.Ordinal))
            return false;
        if (jwk.TryGetProperty("key_ops", out var operations) && operations.GetArrayLength() > 0)
        {
            var verify = false;
            foreach (var operation in operations.EnumerateArray())
                verify |= string.Equals(operation.GetString(), "verify", StringComparison.Ordinal);
            if (!verify)
                return false;
        }

        return true;
    }

    private static string? Read(JsonElement element, string name) =>
        element.TryGetProperty(name, out var property) ? property.GetString() : null;

    private static byte[] Decode(JsonElement element, string name) =>
        Base64Url.DecodeFromChars(element.GetProperty(name).GetString()!);

    private bool VerifyMac(ReadOnlySpan<byte> input, ReadOnlySpan<byte> signature, HashAlgorithmName hash)
    {
        Span<byte> mac = stackalloc byte[HMACSHA512.HashSizeInBytes];
        var written = CryptographicOperations.HmacData(hash, _material, input, mac);
        return CryptographicOperations.FixedTimeEquals(mac[..written], signature);
    }

    private bool VerifyRsa(ReadOnlySpan<byte> input, ReadOnlySpan<byte> signature, HashAlgorithmName hash, char family)
    {
        if (signature.Length != (_bits + 7) / 8)
            return false;
        using var rsa = RSA.Create();
        rsa.ImportSubjectPublicKeyInfo(_material, out _);
        return rsa.VerifyData(input, signature, hash, family == 'P' ? RSASignaturePadding.Pss : RSASignaturePadding.Pkcs1);
    }

    private bool VerifyEc(ReadOnlySpan<byte> input, ReadOnlySpan<byte> signature, HashAlgorithmName hash)
    {
        var fieldBytes = (_bits + 7) / 8;
        if (signature.Length != 2 * fieldBytes)
            return false;
        using var ec = ECDsa.Create();
        ec.ImportSubjectPublicKeyInfo(_material, out _);
        return ec.VerifyData(input, signature, hash, DSASignatureFormat.IeeeP1363FixedFieldConcatenation);
    }
}
