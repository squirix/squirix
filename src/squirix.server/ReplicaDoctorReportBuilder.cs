using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage.Replication;

namespace Squirix.Server;

/// <summary>Builds offline read-only replica diagnostics for the server doctor command.</summary>
/// <remarks>
/// The builder compares the configured topology identity against the activated stamp and the durable
/// per-group metadata without opening logs, advancing terms, or writing anything. Expected identity
/// arrives as primitives so this namespace never depends upward on cluster configuration types.
/// </remarks>
[Immutable]
internal static class ReplicaDoctorReportBuilder
{
    /// <summary>Builds offline replica diagnostics from durable state only.</summary>
    /// <param name="expectedFingerprintHex">The configured topology fingerprint as uppercase hex.</param>
    /// <param name="expectedGeneration">The configured configuration generation.</param>
    /// <param name="replicaCount">The configured replica factor.</param>
    /// <param name="groupIds">The replica group identifiers served by the node.</param>
    /// <param name="dataDirectory">The node data directory holding the stamp and group metadata.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The mismatch flag and the stable diagnostic lines for doctor output.</returns>
    internal static async Task<(bool HasMismatch, List<string> Lines)> BuildAsync(
        string expectedFingerprintHex,
        ulong expectedGeneration,
        int replicaCount,
        IReadOnlyList<string> groupIds,
        string dataDirectory,
        CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrEmpty(expectedFingerprintHex);
        ArgumentNullException.ThrowIfNull(groupIds);
        ArgumentException.ThrowIfNullOrEmpty(dataDirectory);

        var expectedBytes = Convert.FromHexString(expectedFingerprintHex);
        var lines = new List<string>
        {
            $"topology fingerprint: {expectedFingerprintHex}",
            $"configuration generation: {expectedGeneration.ToString(CultureInfo.InvariantCulture)}",
        };

        // Both appends have side effects (report lines) and must always run: no short-circuiting here.
        var stampMismatch = await AppendStampLinesAsync(dataDirectory, expectedGeneration, replicaCount, expectedBytes, lines, cancellationToken).ConfigureAwait(false);
        var groupMismatch = await AppendGroupLinesAsync(dataDirectory, groupIds, expectedBytes, expectedGeneration, lines, cancellationToken).ConfigureAwait(false);
        var mismatch = stampMismatch || groupMismatch;

        return (mismatch, lines);
    }

    private static void AppendGroupLine(List<string> lines, string groupId, in GroupLogMetadata meta, ReadOnlySpan<byte> expectedFingerprint, ulong expectedGeneration)
    {
        var fingerprintMatch = ReplicaTopologyMatch.MatchesFingerprint(meta.TopologyFingerprint, expectedFingerprint);
        var generationMatch = ReplicaTopologyMatch.MatchesGeneration(meta.ConfigurationGeneration, expectedGeneration);
        var applyLag = meta.CommitIndex >= meta.LastAppliedIndex ? meta.CommitIndex - meta.LastAppliedIndex : 0UL;
        lines.Add(
            $"group '{groupId}': term {meta.CurrentTerm.ToString(CultureInfo.InvariantCulture)}" + $" commit {meta.CommitIndex.ToString(CultureInfo.InvariantCulture)}" +
            $" applied {meta.LastAppliedIndex.ToString(CultureInfo.InvariantCulture)}" + $" apply-lag {applyLag.ToString(CultureInfo.InvariantCulture)}" +
            $" fingerprint {(fingerprintMatch ? "match" : "MISMATCH")}" + $" generation {(generationMatch ? "match" : "MISMATCH")}");
    }

    private static async Task<bool> AppendGroupLinesAsync(
        string dataDirectory,
        IReadOnlyList<string> groupIds,
        byte[] expectedFingerprint,
        ulong expectedGeneration,
        List<string> lines,
        CancellationToken cancellationToken)
    {
        var mismatch = false;
        for (var i = 0; i < groupIds.Count; i++)
        {
            var groupId = groupIds[i];
            var metadataPath = GroupStoragePaths.GetMetadataPath(dataDirectory, groupId);
            if (!File.Exists(metadataPath))
            {
                lines.Add($"group '{groupId}': no durable state");
                continue;
            }

            byte[] bytes;
            try
            {
                bytes = await File.ReadAllBytesAsync(metadataPath, cancellationToken).ConfigureAwait(false);
            }
            catch (FileNotFoundException)
            {
                lines.Add($"group '{groupId}': no durable state");
                continue;
            }

            if (!GroupLogCodec.TryDecodeMeta(bytes, out var meta))
            {
                lines.Add($"group '{groupId}': metadata UNREADABLE (checksum or format mismatch)");
                mismatch = true;
                continue;
            }

            AppendGroupLine(lines, groupId, in meta, expectedFingerprint, expectedGeneration);
            if (!ReplicaTopologyMatch.MatchesFingerprint(meta.TopologyFingerprint, expectedFingerprint) ||
                !ReplicaTopologyMatch.MatchesGeneration(meta.ConfigurationGeneration, expectedGeneration))
                mismatch = true;
        }

        return mismatch;
    }

    private static async Task<bool> AppendStampLinesAsync(
        string dataDirectory,
        ulong expectedGeneration,
        int replicaCount,
        byte[] expectedFingerprint,
        List<string> lines,
        CancellationToken cancellationToken)
    {
        ActivatedTopologyStamp? stamped;
        try
        {
            stamped = await new ActivatedTopologyStampStore(dataDirectory).ReadAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException ex)
        {
            lines.Add($"topology stamp: UNREADABLE ({ex.Message})");
            return true;
        }

        if (stamped == null)
        {
            lines.Add("topology stamp: not activated");
            return false;
        }

        var mismatch = false;
        if (stamped.Generation != expectedGeneration)
        {
            lines.Add(
                $"topology stamp: generation MISMATCH (stamped {stamped.Generation.ToString(CultureInfo.InvariantCulture)}, configured {expectedGeneration.ToString(CultureInfo.InvariantCulture)})");
            mismatch = true;
        }
        else
        {
            lines.Add("topology stamp: generation match");
        }

        if (stamped.ReplicaCount != replicaCount)
        {
            lines.Add(
                $"topology stamp: replica count MISMATCH (stamped {stamped.ReplicaCount.ToString(CultureInfo.InvariantCulture)}, configured {replicaCount.ToString(CultureInfo.InvariantCulture)})");
            mismatch = true;
        }
        else
        {
            lines.Add("topology stamp: replica count match");
        }

        if (!stamped.Fingerprint.Span.SequenceEqual(expectedFingerprint))
        {
            lines.Add($"topology stamp: fingerprint MISMATCH (stamped {Convert.ToHexString(stamped.Fingerprint.Span)}, configured {Convert.ToHexString(expectedFingerprint)})");
            mismatch = true;
        }
        else
        {
            lines.Add("topology stamp: fingerprint match");
        }

        return mismatch;
    }
}
