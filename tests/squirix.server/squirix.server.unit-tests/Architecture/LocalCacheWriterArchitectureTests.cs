using System;
using System.Collections.Generic;
using System.IO;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.UnitTests.SourceScanning;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>
/// Keeps every writer of the local cache behind the replica committer: only the composition roots, the local cache itself, recovery,
/// and the journal path may name the symbols that reach memory or the journal without going through a replicated commit.
/// </summary>
[Immutable]
public sealed partial class LocalCacheWriterArchitectureTests : ServerUnitTestBase
{
    private const int MatchTimeoutMilliseconds = 2000;

    private static readonly WriterRule[] Rules =
    [
        new(
            LocalChainKeyPattern,
            ["Node/Hosting/CachePipelineRegistration.cs", "Node/Hosting/ServerHostingComposition.cs"],
            "The keyed local chain bypasses the replicated pipeline; only the composition roots resolve it."),
        new(
            LocalCacheTypesPattern,
            [
                "LocalCache/",
                "Node/Hosting/RuntimeServiceRegistration.cs",
                "Node/Hosting/CachePipelineRegistration.cs",
                "Node/Hosting/PersistenceServiceRegistration.cs",
                "Node/Services/RecoveryService.cs",
                "Node/Services/RecoveryDependencies.cs",
            ],
            "Local cache types are written to only by the local cache, recovery, and the composition roots."),
        new(
            PipelineConstructionPattern,
            ["Node/Hosting/CachePipelineRegistration.cs"],
            "The cache pipeline, including its replicated stage, is assembled in one place."),
        new(
            CommittedApplyPattern,
            ["Node/Services/ReplicaLeaderApplier.cs", "Node/Services/ReplicaCacheApplier.cs"],
            "Committed records reach memory only through the ordered leader applier."),
        new(
            JournalAppendPattern,
            [
                "Node/App/Decorators/JournalLoggingCacheDecorator.cs",
                "Node/Observability/TracingJournalCoordinatorDecorator.cs",
                "Storage/Journaling/",
            ],
            "Journal frames are appended only by the journaling decorator and the journal itself."),
    ];

    [GeneratedRegex(@"\bLocalChainKey\b", RegexOptions.CultureInvariant, MatchTimeoutMilliseconds)]
    private static partial Regex LocalChainKeyPattern { get; }

    [GeneratedRegex(@"\b(?:ILocalCache|ILocalCacheMutationOperations|ILocalCacheRecovery|PhysicalCache|ClientCache)\b", RegexOptions.CultureInvariant, MatchTimeoutMilliseconds)]
    private static partial Regex LocalCacheTypesPattern { get; }

    [GeneratedRegex(@"\bnew\s+(?:ReplicatedCache|JournalLoggingCacheDecorator|JournalPayloadPrepareCacheDecorator|OwnerPutPayloadGuardDecorator|ClientCache)\b", RegexOptions.CultureInvariant, MatchTimeoutMilliseconds)]
    private static partial Regex PipelineConstructionPattern { get; }

    [GeneratedRegex(@"\bReplicaCacheApplier\s*\.\s*ExecuteAsync\b", RegexOptions.CultureInvariant, MatchTimeoutMilliseconds)]
    private static partial Regex CommittedApplyPattern { get; }

    [GeneratedRegex(@"\.\s*Append(?:Put|Remove)Async\s*\(", RegexOptions.CultureInvariant, MatchTimeoutMilliseconds)]
    private static partial Regex JournalAppendPattern { get; }

    /// <summary>Ensures owned-key writers are named only by the files that sit behind the replica committer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnedKeyWritersStayBehindCommitter(CancellationToken cancellationToken)
    {
        var files = await ServerSourceFiles.EnumerateCsharpFilesAsync();
        var serverRoot = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server");
        var violations = new List<string>();
        var matchedAllowed = new bool[Rules.Length];
        for (var fileIndex = 0; fileIndex < files.Count; fileIndex++)
        {
            var path = files[fileIndex];
            var relative = Path.GetRelativePath(serverRoot, path).Replace(Path.DirectorySeparatorChar, '/');
            var masked = CsharpSourceMasker.Mask(await File.ReadAllTextAsync(path, cancellationToken));
            for (var ruleIndex = 0; ruleIndex < Rules.Length; ruleIndex++)
            {
                var rule = Rules[ruleIndex];
                if (!rule.Pattern.IsMatch(masked))
                    continue;

                if (rule.Allows(relative))
                    matchedAllowed[ruleIndex] = true;
                else
                    violations.Add($"{relative}: {rule.Reason}");
            }
        }

        for (var ruleIndex = 0; ruleIndex < Rules.Length; ruleIndex++)
        {
            if (!matchedAllowed[ruleIndex])
                violations.Add($"Rule '{Rules[ruleIndex].Pattern}' matches no allowed file and no longer protects anything.");
        }

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>A banned-symbol pattern, the source paths allowed to match it, and why the rest may not.</summary>
    /// <param name="Pattern">The pattern matched against masked source.</param>
    /// <param name="Allowed">Paths relative to the server project; an entry ending in a slash allows the whole directory.</param>
    /// <param name="Reason">Why other files must not match.</param>
    [Immutable]
    private readonly record struct WriterRule(Regex Pattern, string[] Allowed, string Reason)
    {
        internal bool Allows(string relativePath)
        {
            for (var index = 0; index < Allowed.Length; index++)
            {
                var entry = Allowed[index];
                if (entry.EndsWith('/') ? relativePath.StartsWith(entry, StringComparison.Ordinal) : string.Equals(relativePath, entry, StringComparison.Ordinal))
                    return true;
            }

            return false;
        }
    }
}
