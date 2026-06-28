using System;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Normalizes HTTPS listen URLs for peer matching and cluster configuration in tests.</summary>
public static class ListenUris
{
    /// <summary>Returns a stable authority URL without a trailing path segment.</summary>
    /// <param name="uri">The listen URL.</param>
    /// <returns>A canonical authority URL.</returns>
    public static string CanonicalAuthority(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        return new UriBuilder(uri.Scheme, uri.Host, uri.Port).Uri.GetLeftPart(UriPartial.Authority);
    }

    /// <summary>Determines whether a listen URL and authority string refer to the same host and port.</summary>
    /// <param name="left">The configured URL.</param>
    /// <param name="right">The authority string.</param>
    /// <returns><see langword="true" /> when authorities match.</returns>
    public static bool SameAuthority(Uri left, string right)
    {
        ArgumentNullException.ThrowIfNull(left);
        return SameAuthority(CanonicalAuthority(left), right);
    }

    /// <summary>Determines whether two listen URLs refer to the same host and port.</summary>
    /// <param name="left">The first URL.</param>
    /// <param name="right">The second URL.</param>
    /// <returns><see langword="true" /> when authorities match.</returns>
    private static bool SameAuthority(string left, string right) => string.Equals(
        CanonicalAuthority(new Uri(left, UriKind.Absolute)),
        CanonicalAuthority(new Uri(right, UriKind.Absolute)),
        StringComparison.OrdinalIgnoreCase);
}
