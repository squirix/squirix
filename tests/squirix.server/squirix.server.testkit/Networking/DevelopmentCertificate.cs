using System;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Networking;

/// <summary>The .NET SDK development HTTPS certificate, loaded once and copied for each test node.</summary>
/// <remarks>
/// Store queries serialize across threads. Sharing the lookup avoids delaying simultaneous node starts.
/// Each node owns its own certificate instance so concurrent listeners do not share mutable certificate state.
/// </remarks>
[ThreadSafe]
internal static class DevelopmentCertificate
{
    /// <summary>The extension that marks a certificate as the .NET SDK development HTTPS certificate.</summary>
    private const string MarkerOid = "1.3.6.1.4.1.311.84.1.1";

    private static readonly Lazy<X509Certificate2?> Certificate = new(Find);

    internal static X509Certificate2? Shared => Certificate.Value;

    /// <summary>Finds the valid development certificate with a private key and the highest marker version.</summary>
    /// <returns>The certificate, or <see langword="null" /> when the current user has none or the platform is macOS.</returns>
    /// <remarks>
    /// On macOS the store is the login keychain, and a TLS handshake with a key taken from it hangs on a runner without a user session.
    /// On macOS CI, the native listener uses the exported PFX configured by the certificate setup step.
    /// </remarks>
    private static X509Certificate2? Find()
    {
        if (OperatingSystem.IsMacOS())
            return null;

        using var store = new X509Store(StoreName.My, StoreLocation.CurrentUser);
        store.Open(OpenFlags.ReadOnly);
        var now = DateTime.Now;
        X509Certificate2? best = null;
        var bestVersion = -1;
        foreach (var candidate in store.Certificates)
        {
            var version = MarkerVersion(candidate);
            if (version > bestVersion && candidate.HasPrivateKey && candidate.NotBefore <= now && now <= candidate.NotAfter)
            {
                best?.Dispose();
                best = candidate;
                bestVersion = version;
            }
            else
            {
                candidate.Dispose();
            }
        }

        return best;
    }

    /// <summary>Reads the development certificate version from its marker extension.</summary>
    /// <param name="certificate">The certificate.</param>
    /// <returns>The version, or -1 when the certificate is not a development certificate.</returns>
    private static int MarkerVersion(X509Certificate2 certificate)
    {
        foreach (var extension in certificate.Extensions)
        {
            if (string.Equals(extension.Oid?.Value, MarkerOid, StringComparison.Ordinal))
                return extension.RawData.Length > 0 ? extension.RawData[0] : 0;
        }

        return -1;
    }

    /// <summary>The development certificate copy of one node; the copy keeps the private key and is disposed with the node's services.</summary>
    [Immutable]
    internal sealed class NodeCertificate : IDisposable
    {
        /// <summary>Initializes a new instance of the <see cref="NodeCertificate" /> class.</summary>
        /// <param name="source">The certificate to copy.</param>
        internal NodeCertificate(X509Certificate2 source)
        {
            Value = new X509Certificate2(source);
        }

        /// <summary>Gets the certificate of the node.</summary>
        internal X509Certificate2 Value { get; }

        /// <inheritdoc />
        public void Dispose() => Value.Dispose();
    }
}
