using System;
using System.Collections.Generic;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class JwtValidationOptions
{
    internal string? Audience { get; set; }

    internal string? Issuer { get; set; }

    internal IReadOnlyList<string> AdditionalIssuers { get; set; } = [];

    internal JwtVerificationKey? SigningKey { get; set; }

    internal IJwtMetadataSource? Metadata { get; set; }

    internal bool RequireHttpsMetadata { get; set; }

    internal bool AcceptsIssuer(string issuer, JwtMetadata? metadata)
    {
        if (string.IsNullOrEmpty(issuer))
            return false;
        if (string.Equals(issuer, Issuer, StringComparison.Ordinal) || string.Equals(issuer, metadata?.Issuer, StringComparison.Ordinal))
            return true;
        foreach (var alternate in AdditionalIssuers)
        {
            if (string.Equals(issuer, alternate, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
