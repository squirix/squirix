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

    /// <summary>Determines whether two listen URLs refer to the same host and port.</summary>
    /// <param name="left">The first URL.</param>
    /// <param name="right">The second URL.</param>
    /// <returns><see langword="true" /> when authorities match.</returns>
    public static bool SameAuthority(Uri left, Uri right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        return string.Equals(CanonicalAuthority(left), CanonicalAuthority(right), StringComparison.OrdinalIgnoreCase);
    }
}
