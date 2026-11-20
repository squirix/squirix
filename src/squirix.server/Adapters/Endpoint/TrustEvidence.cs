namespace Squirix.Server.Adapters.Endpoint;

internal readonly record struct TrustEvidence(string Issuer, JwtVerificationKey SigningKey);
