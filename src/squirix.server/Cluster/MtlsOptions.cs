using System;
using System.Collections.Generic;
using System.IO;
using Squirix.Attributes;

namespace Squirix.Server.Cluster;

/// <summary>Cluster-scoped inter-node mTLS configuration. Does not affect external client authentication.</summary>
[Immutable]
internal sealed record MtlsOptions
{
    /// <summary>Gets the path to the PEM-encoded cluster CA / trust root certificate.</summary>
    internal string? CaPath { get; init; }

    /// <summary>Gets the path to the PEM-encoded node certificate.</summary>
    internal string? CertPath { get; init; }

    /// <summary>
    /// Gets the optional password for <see cref="CertPfxPath" />.
    /// </summary>
    internal string? CertPfxPassword { get; init; }

    /// <summary>Gets the path to the node certificate PFX/PKCS#12 file.</summary>
    internal string? CertPfxPath { get; init; }

    /// <summary>Gets the dedicated cluster/internal HTTPS listener port for inter-node mTLS.</summary>
    internal int InternalListenPort { get; init; }

    /// <summary>Gets the path to the PEM-encoded node private key.</summary>
    internal string? KeyPath { get; init; }

    /// <summary>Validates configuration shape and file presence without loading certificates.</summary>
    /// <param name="primaryListenPort">Primary external HTTPS listener port.</param>
    /// <param name="requiresInterNodeMtls">Whether cluster topology requires inter-node mTLS.</param>
    /// <exception cref="InvalidOperationException">Thrown when configuration is incomplete or inconsistent.</exception>
    internal void Validate(int? primaryListenPort, bool requiresInterNodeMtls)
    {
        if (!requiresInterNodeMtls)
            return;

        var failures = new List<string>();
        CollectCaFailures(failures);
        CollectCredentialFailures(failures);
        CollectPortFailures(primaryListenPort, failures);

        if (failures.Count > 0)
            throw new InvalidOperationException(string.Join(' ', failures));
    }

    private void CollectCaFailures(List<string> failures)
    {
        // Validation is shape-only: confirm required files exist without loading certificates into memory.
        if (string.IsNullOrWhiteSpace(CaPath))
            failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_CA_PATH when cluster peers are configured.");
        else if (!File.Exists(CaPath))
            failures.Add("Cluster mTLS CA file was not found.");
    }

    private void CollectCredentialFailures(List<string> failures)
    {
        var hasPfx = !string.IsNullOrWhiteSpace(CertPfxPath);
        var hasPemCert = !string.IsNullOrWhiteSpace(CertPath);
        var hasPemKey = !string.IsNullOrWhiteSpace(KeyPath);

        if (hasPfx && (hasPemCert || hasPemKey))
            failures.Add("Cluster mTLS must use either SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH or PEM cert/key paths, not both.");

        if (!hasPfx && !hasPemCert && !hasPemKey)
        {
            failures.Add(
                "Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_CERT_PFX_PATH or SQUIRIX_CLUSTER_MTLS_CERT_PATH and SQUIRIX_CLUSTER_MTLS_KEY_PATH when cluster peers are configured.");
            return;
        }

        if (hasPfx)
        {
            CollectPfxFailures(failures);
            return;
        }

        CollectPemFailures(hasPemCert, hasPemKey, failures);
    }

    private void CollectPfxFailures(List<string> failures)
    {
        if (!File.Exists(CertPfxPath))
            failures.Add("Cluster mTLS PFX file was not found.");
    }

    private void CollectPemFailures(bool hasPemCert, bool hasPemKey, List<string> failures)
    {
        if (!hasPemCert)
            failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_CERT_PATH when PEM mode is used.");
        else if (!File.Exists(CertPath))
            failures.Add("Cluster mTLS certificate file was not found.");

        if (!hasPemKey)
            failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_KEY_PATH when PEM mode is used.");
        else if (!File.Exists(KeyPath))
            failures.Add("Cluster mTLS private key file was not found.");
    }

    private void CollectPortFailures(int? primaryListenPort, List<string> failures)
    {
        if (InternalListenPort <= 0)
            failures.Add("Cluster mTLS requires SQUIRIX_CLUSTER_MTLS_INTERNAL_PORT when cluster peers are configured.");

        // Internal cluster listener must not collide with the external client HTTPS port.
        if (primaryListenPort is > 0 && InternalListenPort == primaryListenPort)
            failures.Add("Cluster mTLS internal listen port must differ from the primary HTTPS listener port.");
    }
}
