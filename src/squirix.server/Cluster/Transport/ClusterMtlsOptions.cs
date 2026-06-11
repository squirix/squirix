using System;
using System.Collections.Generic;
using System.IO;

namespace Squirix.Server.Cluster.Transport;

/// <summary>
/// Cluster-scoped inter-node mTLS configuration. Does not affect external client authentication.
/// </summary>
internal sealed record ClusterMtlsOptions
{
    /// <summary>
    /// Gets a value indicating whether inter-node cluster mTLS is enabled.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>
    /// Gets the path to the node certificate PFX/PKCS#12 file.
    /// </summary>
    public string? CertPfxPath { get; init; }

    /// <summary>
    /// Gets the optional password for <see cref="CertPfxPath" />.
    /// </summary>
    public string? CertPfxPassword { get; init; }

    /// <summary>
    /// Gets the path to the PEM-encoded node certificate.
    /// </summary>
    public string? CertPath { get; init; }

    /// <summary>
    /// Gets the path to the PEM-encoded node private key.
    /// </summary>
    public string? KeyPath { get; init; }

    /// <summary>
    /// Gets the path to the PEM-encoded cluster CA / trust root certificate.
    /// </summary>
    public string? CaPath { get; init; }

    /// <summary>
    /// Validates configuration shape and file presence without loading certificates.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when configuration is incomplete or inconsistent.</exception>
    public void Validate()
    {
        if (!Enabled)
            return;

        var failures = new List<string>();
        var hasPfx = !string.IsNullOrWhiteSpace(CertPfxPath);
        var hasPemCert = !string.IsNullOrWhiteSpace(CertPath);
        var hasPemKey = !string.IsNullOrWhiteSpace(KeyPath);

        if (string.IsNullOrWhiteSpace(CaPath))
            failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_CA_PATH when enabled.");
        else if (!File.Exists(CaPath))
            failures.Add($"Cluster mTLS CA file was not found: '{CaPath}'.");

        if (hasPfx && (hasPemCert || hasPemKey))
            failures.Add("Cluster mTLS must use either SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH or PEM cert/key paths, not both.");

        if (!hasPfx && !hasPemCert && !hasPemKey)
            failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH or SQUIRIX_CLUSTER_MTLS_CERT_PATH and SQUIRIX_CLUSTER_MTLS_KEY_PATH when enabled.");

        if (hasPfx)
        {
            if (!File.Exists(CertPfxPath!))
                failures.Add($"Cluster mTLS PFX file was not found: '{CertPfxPath}'.");
        }
        else
        {
            if (!hasPemCert)
                failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_CERT_PATH when PEM mode is used.");
            else if (!File.Exists(CertPath!))
                failures.Add($"Cluster mTLS certificate file was not found: '{CertPath}'.");

            if (!hasPemKey)
                failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_KEY_PATH when PEM mode is used.");
            else if (!File.Exists(KeyPath!))
                failures.Add($"Cluster mTLS private key file was not found: '{KeyPath}'.");
        }

        if (failures.Count > 0)
            throw new InvalidOperationException(string.Join(' ', failures));
    }
}
