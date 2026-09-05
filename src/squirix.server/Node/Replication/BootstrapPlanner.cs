using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Replication;

/// <summary>Validates and durably prepares a stopped RF=1 cluster for replica seeding.</summary>
[SuppressMessage("StyleCop.CSharp.OrderingRules", "SA1204:Static members should appear before non-static members", Justification = "Preparation entry point precedes its validation helpers for readability.")]
internal sealed class BootstrapPlanner
{
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="BootstrapPlanner" /> class.</summary>
    /// <param name="timeProvider">Time source used to report legacy retention boundaries.</param>
    internal BootstrapPlanner(TimeProvider? timeProvider = null)
    {
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Prepares or resumes a checksummed bootstrap manifest under exclusive data-directory ownership.</summary>
    /// <param name="request">Validated source, target, persistence, and legacy outcome inputs.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The created or resumed manifest.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="request" /> is null.</exception>
    /// <exception cref="InvalidOperationException">Thrown when topology requirements fail, ownership is unavailable, or an existing manifest targets a different migration.</exception>
    /// <exception cref="InvalidDataException">Thrown when an existing manifest is corrupt.</exception>
    internal async Task<BootstrapPreparationResult> PrepareAsync(BootstrapPreparationRequest request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        ValidateRequest(request);
        var persistence = request.Persistence!;
        _ = await DirectoryEx.CreateDirectoryAsync(persistence.DataDir, cancellationToken: cancellationToken).ConfigureAwait(false);
        var lockPath = PathEx.Combine(persistence.DataDir, "bootstrap.lock");
        SafeFileHandle ownership;
        try
        {
            ownership = File.OpenHandle(lockPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, FileOptions.WriteThrough);
        }
        catch (IOException exception)
        {
            throw new InvalidOperationException("Bootstrap requires exclusive ownership of the stopped data directory.", exception);
        }

        using (ownership)
        {
            cancellationToken.ThrowIfCancellationRequested();
            ValidateLegacyOutcomes(request.LegacyOutcomes, _timeProvider.GetUtcNow());
            var candidate = CreateManifest(request);
            var store = new BootstrapManifestStore(persistence.DataDir);
            var existing = await store.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (existing != null)
            {
                if (!MatchesAttempt(existing, candidate))
                    throw new InvalidOperationException("Existing bootstrap manifest targets a different topology or generation.");
                return new BootstrapPreparationResult(existing, true, store.ManifestPath);
            }

            await store.PublishAsync(candidate, cancellationToken).ConfigureAwait(false);
            return new BootstrapPreparationResult(candidate, false, store.ManifestPath);
        }
    }

    internal static BootstrapManifest CreateManifest(BootstrapPreparationRequest request)
    {
        var sourceFingerprint = GC.AllocateUninitializedArray<byte>(32);
        var targetFingerprint = GC.AllocateUninitializedArray<byte>(32);
        TopologyFingerprint.CreateFromTopology(request.SourceTopology, request.SourceMtls).Bytes.CopyTo(sourceFingerprint);
        TopologyFingerprint.CreateFromTopology(request.TargetTopology, request.TargetMtls).Bytes.CopyTo(targetFingerprint);
        var groups = new List<BootstrapGroupProgress>(request.GroupIds.Count);
        var unique = new HashSet<string>(StringComparer.Ordinal);
        for (var index = 0; index < request.GroupIds.Count; index++)
        {
            var groupId = request.GroupIds[index];
            if (string.IsNullOrWhiteSpace(groupId))
                throw new InvalidOperationException("Bootstrap group identity cannot be empty.");
            if (!unique.Add(groupId))
                throw new InvalidOperationException($"Duplicate bootstrap group '{groupId}'.");
            groups.Add(new BootstrapGroupProgress(groupId, BootstrapGroupState.Pending));
        }

        return new BootstrapManifest
        {
            Groups = groups,
            SourceClusterId = request.SourceTopology.ClusterId,
            SourceFingerprint = sourceFingerprint,
            SourceGeneration = request.SourceTopology.ConfigurationGeneration,
            TargetFingerprint = targetFingerprint,
            TargetGeneration = request.TargetTopology.ConfigurationGeneration,
            TargetReplicaCount = request.TargetTopology.ReplicaCount,
        };
    }

    internal static bool MatchesAttempt(BootstrapManifest existing, BootstrapManifest candidate)
    {
        if (!string.Equals(existing.SourceClusterId, candidate.SourceClusterId, StringComparison.Ordinal) || existing.SourceGeneration != candidate.SourceGeneration ||
            existing.TargetGeneration != candidate.TargetGeneration || existing.TargetReplicaCount != candidate.TargetReplicaCount ||
            !existing.SourceFingerprint.Span.SequenceEqual(candidate.SourceFingerprint.Span) || !existing.TargetFingerprint.Span.SequenceEqual(candidate.TargetFingerprint.Span) ||
            existing.Groups.Count != candidate.Groups.Count)
            return false;

        for (var index = 0; index < existing.Groups.Count; index++)
        {
            if (!string.Equals(existing.Groups[index].GroupId, candidate.Groups[index].GroupId, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    internal static bool PeersMatch(ServerPeer[] source, ServerPeer[] target)
    {
        if (source.Length != target.Length)
            return false;
        for (var index = 0; index < source.Length; index++)
        {
            var left = source[index];
            var right = target[index];
            if (!string.Equals(left.NodeId, right.NodeId, StringComparison.Ordinal) || !UriEquals(left.Uri, right.Uri) || !NullableUriEquals(left.InterNodeUri, right.InterNodeUri))
                return false;
        }

        return true;
    }

    internal static bool NullableUriEquals(Uri? source, Uri? target) => source == null ? target == null : target != null && UriEquals(source, target);

    internal static bool UriEquals(Uri source, Uri target) => string.Equals(source.AbsoluteUri, target.AbsoluteUri, StringComparison.Ordinal);

    private static void ValidateLegacyOutcomes(IReadOnlyList<BootstrapLegacyOutcome> outcomes, DateTimeOffset now)
    {
        ArgumentNullException.ThrowIfNull(outcomes);
        DateTimeOffset? safeRetry = null;
        var blocked = false;
        for (var index = 0; index < outcomes.Count; index++)
        {
            var outcome = outcomes[index];
            if (outcome.HasReplicaGroupScope)
                continue;
            blocked = true;
            if (outcome.ExpiresUtc is { } expires && (safeRetry == null || expires > safeRetry.Value))
                safeRetry = expires;
        }

        if (!blocked)
            return;
        var retry = safeRetry is { } value && value > now ? $" Earliest safe retry: {value:O}." : string.Empty;
        throw new InvalidOperationException($"RF bootstrap cannot infer a replica group from an opaque legacy idempotency fingerprint.{retry}");
    }

    private static void ValidateRequest(BootstrapPreparationRequest request)
    {
        ArgumentNullException.ThrowIfNull(request.SourceTopology);
        ArgumentNullException.ThrowIfNull(request.TargetTopology);
        ArgumentNullException.ThrowIfNull(request.SourceMtls);
        ArgumentNullException.ThrowIfNull(request.TargetMtls);
        ArgumentNullException.ThrowIfNull(request.GroupIds);
        if (request.Persistence == null || string.IsNullOrWhiteSpace(request.Persistence.DataDir))
            throw new InvalidOperationException("RF bootstrap requires persistence and a data directory.");
        if (request.SourceTopology.ReplicaCount != 1)
            throw new InvalidOperationException("RF bootstrap source must have ReplicaCount 1.");
        if (request.TargetTopology.ReplicaCount <= 1)
            throw new InvalidOperationException("RF bootstrap target ReplicaCount must be greater than 1.");
        if (request.TargetTopology.ConfigurationGeneration <= request.SourceTopology.ConfigurationGeneration)
            throw new InvalidOperationException("RF bootstrap target configuration generation must increase.");

        if (!string.Equals(request.SourceTopology.ClusterId, request.TargetTopology.ClusterId, StringComparison.Ordinal) ||
            !string.Equals(request.SourceTopology.NodeId, request.TargetTopology.NodeId, StringComparison.Ordinal) ||
            request.SourceTopology.VirtualNodes != request.TargetTopology.VirtualNodes || !UriEquals(request.SourceTopology.Uri, request.TargetTopology.Uri) ||
            request.SourceMtls.InternalListenPort != request.TargetMtls.InternalListenPort || !PeersMatch(request.SourceTopology.Peers, request.TargetTopology.Peers))
            throw new InvalidOperationException("Only ReplicaCount and ConfigurationGeneration may change during RF bootstrap.");
    }
}
