using System;
using System.Globalization;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Replication;

/// <summary>Durable identity of the activated replica-set topology Frozen at first start.</summary>
[Immutable]
internal sealed class ActivatedTopologyStamp
{
    /// <summary>Gets the stopped-topology configuration generation.</summary>
    internal ulong Generation { get; init; }

    /// <summary>Gets the 32-byte static topology fingerprint.</summary>
    internal required ReadOnlyMemory<byte> Fingerprint { get; init; }

    /// <summary>Gets the replica factor including the original owner.</summary>
    internal int ReplicaCount { get; init; }

    /// <summary>Returns whether another stamp describes the same activated identity.</summary>
    /// <param name="other">The stamp to compare.</param>
    /// <returns><see langword="true" /> when generation, replica count, and fingerprint all match.</returns>
    internal bool Matches(ActivatedTopologyStamp other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Generation == other.Generation && ReplicaCount == other.ReplicaCount && Fingerprint.Span.SequenceEqual(other.Fingerprint.Span);
    }

    /// <summary>Names each activated identity field that differs in a configured identity, with both values.</summary>
    /// <param name="configured">The configured identity compared against this stamp.</param>
    /// <returns>The changed fields as stamped and configured values, or an empty string when the identities match.</returns>
    /// <remarks>
    /// The fingerprint hashes the generation and replica count too, so it is named only when both of them match:
    /// the change then lies in its remaining inputs.
    /// </remarks>
    internal string DescribeChange(ActivatedTopologyStamp configured)
    {
        ArgumentNullException.ThrowIfNull(configured);
        var changes = new StringBuilder();
        if (Generation != configured.Generation)
            AppendChange(changes, "generation", Generation.ToString(CultureInfo.InvariantCulture), configured.Generation.ToString(CultureInfo.InvariantCulture));
        if (ReplicaCount != configured.ReplicaCount)
            AppendChange(changes, "replica count", ReplicaCount.ToString(CultureInfo.InvariantCulture), configured.ReplicaCount.ToString(CultureInfo.InvariantCulture));
        if (changes.Length > 0 || Fingerprint.Span.SequenceEqual(configured.Fingerprint.Span))
            return changes.ToString();

        AppendChange(changes, "topology fingerprint", Convert.ToHexString(Fingerprint.Span), Convert.ToHexString(configured.Fingerprint.Span));
        _ = changes.Append(
            " while generation and replica count match, so the cluster id, virtual nodes, peers (including internode addresses), " +
            "minimum cluster package version, replication policy constants, or the automatic failover and quorum read switches differ");
        return changes.ToString();
    }

    private static void AppendChange(StringBuilder changes, string field, string stamped, string configured)
    {
        if (changes.Length > 0)
            _ = changes.Append("; ");
        _ = changes.Append(field).Append(" changed (stamped ").Append(stamped).Append(", configured ").Append(configured).Append(')');
    }
}
