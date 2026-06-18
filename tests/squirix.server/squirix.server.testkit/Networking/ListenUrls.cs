using System;

namespace Squirix.Server.TestKit.Networking;

/// <summary>Normalizes HTTPS listen URLs for peer matching and cluster configuration in tests.</summary>
public static class ListenUrls
{
    /// <summary>Returns a stable authority URL without a trailing path segment.</summary>
    /// <param name="url">The listen URL.</param>
    /// <returns>A canonical authority URL.</returns>
    public static string CanonicalAuthority(Uri url)
    {
        ArgumentNullException.ThrowIfNull(url);
        return new UriBuilder(url.Scheme, url.Host, url.Port).Uri.GetLeftPart(UriPartial.Authority);
    }

    /// <summary>Determines whether two listen URLs refer to the same host and port.</summary>
    /// <param name="left">The first URL.</param>
    /// <param name="right">The second URL.</param>
    /// <returns><see langword="true" /> when authorities match.</returns>
    public static bool SameAuthority(string left, string right) =>
        string.Equals(
            CanonicalAuthority(new Uri(left, UriKind.Absolute)),
            CanonicalAuthority(new Uri(right, UriKind.Absolute)),
            StringComparison.OrdinalIgnoreCase);
}
