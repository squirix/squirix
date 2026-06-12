using Squirix.Server.Utils;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Resolves cluster mTLS options from process environment variables.
/// </summary>
internal static class ClusterMtlsOptionsResolver
{
    /// <summary>
    /// Loads cluster mTLS options from environment variables.
    /// </summary>
    /// <returns>Resolved options.</returns>
    public static ClusterMtlsOptions ResolveFromEnvironment() =>
        new()
        {
            CertPfxPath = NormalizePath(EnvVariables.ReadString("SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH")),
            CertPfxPassword = EnvVariables.ReadString("SQUIRIX_CLUSTER_MTLS_CERT_PFX_PASSWORD"),
            CertPath = NormalizePath(EnvVariables.ReadString("SQUIRIX_CLUSTER_MTLS_CERT_PATH")),
            KeyPath = NormalizePath(EnvVariables.ReadString("SQUIRIX_CLUSTER_MTLS_KEY_PATH")),
            CaPath = NormalizePath(EnvVariables.ReadString("SQUIRIX_CLUSTER_MTLS_CA_PATH")),
            InternalListenPort = EnvVariables.ReadInt("SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT") ?? 0,
        };

    private static string? NormalizePath(string? value) => string.IsNullOrWhiteSpace(value) ? null : value.Trim();
}
