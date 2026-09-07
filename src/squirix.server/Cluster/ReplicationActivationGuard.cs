using System;
using System.Collections.Generic;

namespace Squirix.Server.Cluster;

/// <summary>
/// Enforces RF&gt;1 startup prerequisites. Persistence and mTLS failures are reported
/// in that order; when both are present RF&gt;1 networking is activated (M8-09).
/// </summary>
internal static class ReplicationActivationGuard
{
    internal const string MtlsRequired = "ReplicaCount greater than 1 requires cluster mTLS material (CA, node certificate, and internal listen port).";

    internal const string PersistenceRequired = "ReplicaCount greater than 1 requires persistence. Call UsePersistence() or pass --persist.";

    /// <summary>
    /// Appends RF&gt;1 activation failures. Missing persistence is reported first, then missing mTLS.
    /// An empty list means RF&gt;1 networking is activated.
    /// </summary>
    /// <param name="failures">Caller-owned failure list.</param>
    /// <param name="replicaCount">Configured replica factor including the original owner.</param>
    /// <param name="persistenceEnabled">Whether journal/snapshot persistence is enabled.</param>
    /// <param name="mtlsConfigured">
    /// When <see langword="null" />, mTLS is not evaluated (public options path); when
    /// <see langword="false" />, mTLS is reported.
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

        if (mtlsConfigured == false)
            failures.Add(MtlsRequired);
    }

    /// <summary>Throws when RF&gt;1 prerequisites are missing for the current hosting setup.</summary>
    /// <param name="replicaCount">Configured replica factor including the original owner.</param>
    /// <param name="persistenceEnabled">Whether journal/snapshot persistence is enabled.</param>
    /// <param name="mtlsOptions">Cluster mTLS options resolved for this node.</param>
    /// <exception cref="InvalidOperationException">Thrown when an RF&gt;1 prerequisite is missing.</exception>
    internal static void ThrowIfDisallowed(int replicaCount, bool persistenceEnabled, MtlsOptions mtlsOptions)
    {
        var failures = new List<string>();
        CollectFailures(failures, replicaCount, persistenceEnabled, IsMtlsConfigured(mtlsOptions));
        if (failures.Count > 0)
            throw new InvalidOperationException(string.Join(' ', failures));
    }

    /// <summary>Returns whether mTLS credential paths and internal port are configured (shape only).</summary>
    /// <param name="options">Cluster mTLS options.</param>
    /// <returns><see langword="true" /> when CA, credentials, and internal port are present.</returns>
    private static bool IsMtlsConfigured(MtlsOptions? options)
    {
        if (options == null)
            return false;

        if (string.IsNullOrWhiteSpace(options.CaPath) || options.InternalListenPort <= 0)
            return false;

        var hasPfx = !string.IsNullOrWhiteSpace(options.CertPfxPath);
        var hasPem = !string.IsNullOrWhiteSpace(options.CertPath) && !string.IsNullOrWhiteSpace(options.KeyPath);
        return hasPfx || hasPem;
    }
}
