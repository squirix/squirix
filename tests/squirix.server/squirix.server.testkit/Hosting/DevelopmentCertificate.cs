using System;
using System.Security.Cryptography.X509Certificates;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// The ASP.NET Core HTTPS development certificate, looked up once per test process and given to every test node, as a copy of its
/// own, as its default server certificate.
/// </summary>
/// <remarks>
/// Without it Kestrel searches the certificate stores for the development certificate on every node start. The store calls
/// serialize across threads, so with many nodes starting at once the search alone held up cluster startups for seconds.
/// Every node gets its own certificate instance: Kestrel decodes the extensions of the certificate lazily when it binds, and that
/// decode is not thread-safe, so nodes binding at once on one shared instance could see no server authentication usage.
/// </remarks>
[ThreadSafe]
internal static class DevelopmentCertificate
{
    /// <summary>The extension that marks a certificate as the ASP.NET Core HTTPS development certificate.</summary>
    private const string MarkerOid = "1.3.6.1.4.1.311.84.1.1";

    private static readonly Lazy<X509Certificate2?> Certificate = new(Find);

    /// <summary>Makes the development certificate the default server certificate of the node's HTTPS endpoints.</summary>
    /// <param name="services">The services of the node; the configuration goes first, so it applies before the node configures its listeners.</param>
    /// <remarks>An endpoint that sets its own certificate keeps it. Without a development certificate Kestrel falls back to its own search.</remarks>
    internal static void AddTo(IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var shared = Certificate.Value;
        if (shared == null)
            return;

        _ = services.AddSingleton(_ => new NodeCertificate(shared));
        _ = services.AddOptions<KestrelServerOptions>()
            .Configure<NodeCertificate>(static (kestrel, node) => kestrel.ConfigureHttpsDefaults(https => https.ServerCertificate ??= node.Value));
    }

    /// <summary>Finds the valid development certificate with a private key and the highest version, as Kestrel does.</summary>
    /// <returns>The certificate, or <see langword="null" /> when the current user has none.</returns>
    private static X509Certificate2? Find()
    {
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
