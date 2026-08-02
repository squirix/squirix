using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster;

/// <summary>
/// Enforces RF&gt;1 startup prerequisites and refuses network activation until M8-09.
/// Persistence and mTLS failures are reported before the activation refusal.
/// </summary>
internal static class ReplicationActivationGuard
{
    internal const string MtlsRequired =
        "ReplicaCount greater than 1 requires cluster mTLS material (CA, node certificate, and internal listen port).";

    internal const string NotActivated =
        "ReplicaCount greater than 1 is not activated until replication activation (M8-09).";

    internal const string PersistenceRequired =
        "ReplicaCount greater than 1 requires persistence. Call UsePersistence() or pass --persist.";

    /// <summary>Returns whether mTLS credential paths and internal port are configured (shape only).</summary>
    /// <param name="options">Cluster mTLS options.</param>
    /// <returns><see langword="true" /> when CA, credentials, and internal port are present.</returns>
    internal static bool IsMtlsConfigured(MtlsOptions? options)
    {
        if (options is null)
            return false;

        if (string.IsNullOrWhiteSpace(options.CaPath) || options.InternalListenPort <= 0)
            return false;

        var hasPfx = !string.IsNullOrWhiteSpace(options.CertPfxPath);
        var hasPem = !string.IsNullOrWhiteSpace(options.CertPath) && !string.IsNullOrWhiteSpace(options.KeyPath);
        return hasPfx || hasPem;
    }

    /// <summary>
    /// Appends RF&gt;1 activation failures. Missing persistence is reported first, then missing mTLS,
    /// then the pre-activation refusal when prerequisites are present (or mTLS was not evaluated).
    /// </summary>
    /// <param name="failures">Caller-owned failure list.</param>
    /// <param name="replicaCount">Configured replica factor including the original owner.</param>
    /// <param name="persistenceEnabled">Whether journal/snapshot persistence is enabled.</param>
    /// <param name="mtlsConfigured">
    /// When <see langword="null" />, mTLS is not evaluated (public options path); when
    /// <see langword="false" />, mTLS is reported before activation refusal.
    /// </param>
    internal static void CollectFailures(List<string> failures, int replicaCount, bool persistenceEnabled, bool? mtlsConfigured)
    {
        if (replicaCount <= 1)
            return;

        if (!persistenceEnabled)
        {
            failures.Add(PersistenceRequired);
            return;
        }

        if (mtlsConfigured is false)
        {
            failures.Add(MtlsRequired);
            return;
        }

        failures.Add(NotActivated);
    }

    /// <summary>Throws when RF&gt;1 is not allowed for the current hosting prerequisites.</summary>
    /// <param name="replicaCount">Configured replica factor including the original owner.</param>
    /// <param name="persistenceEnabled">Whether journal/snapshot persistence is enabled.</param>
    /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
    /// <exception cref="InvalidOperationException">Thrown when RF&gt;1 prerequisites fail or activation is refused.</exception>
    internal static void ThrowIfDisallowed(int replicaCount, bool persistenceEnabled, MtlsOptions mtlsOptions)
    {
        var failures = new List<string>();
        CollectFailures(failures, replicaCount, persistenceEnabled, IsMtlsConfigured(mtlsOptions));
        if (failures.Count > 0)
            throw new InvalidOperationException(string.Join(' ', failures));
    }
}
