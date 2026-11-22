using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

[UsedImplicitly]
public sealed class ProtocolModelSurfaceTests
{
    private static readonly string[] SampleCounterexamplePaths = ["start", "elect"];

    [Test]
    public async Task BudgetExceptionCarriesPayload()
    {
        var exception = new TraceSearchBudgetExhaustedException(100, 100);

        _ = await Assert.That(exception.MaxStates).IsEqualTo(100);
        _ = await Assert.That(exception.VisitedStates).IsEqualTo(100);
        _ = await Assert.That(exception.Message).Contains("100 of 100", StringComparison.Ordinal);
    }

    [Test]
    public async Task ExploreProfileForCliBuildsSmallAndFull()
    {
        var small = ExploreProfile.ForCli("small", true);
        _ = await Assert.That(small.Name).IsEqualTo("small", StringComparer.Ordinal);
        _ = await Assert.That(small.Majority).IsEqualTo(2);
        _ = await Assert.That(small.AllowPartition).IsFalse();

        var full = ExploreProfile.ForCli("full", false);
        _ = await Assert.That(full.Name).IsEqualTo("full", StringComparer.Ordinal);
        _ = await Assert.That(full.AllowCrash).IsTrue();
        _ = await Assert.That(full.AllowPartition).IsTrue();
        _ = await Assert.That(full.SymmetryReduce).IsFalse();

        var rf = ExploreProfile.ForReplicaCount(3, 2, 1, 2, 0, true, true);
        _ = await Assert.That(rf.AllowPartition).IsTrue();
        _ = await Assert.That(rf.Majority).IsEqualTo(2);
    }

    [Test]
    public void ExploreProfileForCliRejectsUnknownName() =>
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ExploreProfile.ForCli("tiny", true));

    [Test]
    public void ExploreReplicaCountProfileRejectsRange()
    {
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ExploreProfile.ForReplicaCount(0, 2, 1, 2, 0, false, true));
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ExploreProfile.ForReplicaCount(33, 2, 1, 2, 0, false, true));
    }

    [Test]
    public async Task LogEntryEqualityMatchesTermAndIndex()
    {
        var a = new LogEntry(1, 2);
        var b = new LogEntry(1, 2);
        var c = new LogEntry(2, 2);
        object boxed = b;
        object other = c;
        object wrong = "x";

        _ = await Assert.That(a == b).IsTrue();
        _ = await Assert.That(a != b).IsFalse();
        _ = await Assert.That(a.Equals(boxed)).IsTrue();
        _ = await Assert.That(a.Equals(other)).IsFalse();
        _ = await Assert.That(b.GetHashCode()).IsEqualTo(a.GetHashCode());
        _ = await Assert.That(a.Equals(wrong)).IsFalse();
    }

    /// <summary>Verifies the CLI reports the broken-vote counterexample for the small profile.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunCliBrokenVoteCounterexampleAsync(CancellationToken cancellationToken)
    {
        var output = CreateTempDir();
        try
        {
            var code = await ExploreRunner.RunCliAsync("small", output, BrokenMode.Vote);
            _ = await Assert.That(code).IsEqualTo(0);
            _ = await Assert.That(File.Exists(Path.Join(output, "summary.json"))).IsTrue();
            _ = await Assert.That(File.Exists(Path.Join(output, "counterexample.json"))).IsTrue();
            var summary = await File.ReadAllTextAsync(Path.Join(output, "summary.json"), cancellationToken);
            _ = await Assert.That(summary).Contains("\"broken\":\"Vote\"", StringComparison.Ordinal);
            _ = await Assert.That(summary).Contains("\"invariant\":\"ElectionSafety\"", StringComparison.Ordinal);
            var counterexample = await File.ReadAllTextAsync(Path.Join(output, "counterexample.json"), cancellationToken);
            _ = await Assert.That(counterexample).Contains("\"invariant\":\"ElectionSafety\"", StringComparison.Ordinal);
            _ = await Assert.That(counterexample).Contains("\"path\":[", StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(output, true);
        }
    }

    /// <summary>Verifies the CLI formats commit and read exploration modes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunCliFormatsCommitAndReadModesAsync(CancellationToken cancellationToken)
    {
        var outputCommit = CreateTempDir();
        var outputRead = CreateTempDir();
        try
        {
            // small profile may not hit these invariants; accept either found (0) or missing (3).
            var commitCode = await ExploreRunner.RunCliAsync("small", outputCommit, BrokenMode.CurrentTermCommit);
            var readCode = await ExploreRunner.RunCliAsync("small", outputRead, BrokenMode.ReadIndex);
            _ = await Assert.That(commitCode == 0 || commitCode == 3).IsTrue();
            _ = await Assert.That(readCode == 0 || readCode == 3).IsTrue();

            var commitSummary = await File.ReadAllTextAsync(Path.Join(outputCommit, "summary.json"), cancellationToken);
            var readSummary = await File.ReadAllTextAsync(Path.Join(outputRead, "summary.json"), cancellationToken);
            _ = await Assert.That(commitSummary).Contains("\"broken\":\"CurrentTermCommit\"", StringComparison.Ordinal);
            _ = await Assert.That(readSummary).Contains("\"broken\":\"ReadIndex\"", StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(outputCommit, true);
            Directory.Delete(outputRead, true);
        }
    }

    /// <summary>Verifies the CLI writes a summary for the small profile.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunCliWritesSummaryForSmallProfileAsync(CancellationToken cancellationToken)
    {
        var output = CreateTempDir();
        try
        {
            var code = await ExploreRunner.RunCliAsync("small", output, BrokenMode.None);
            _ = await Assert.That(code).IsEqualTo(0);
            _ = await Assert.That(File.Exists(Path.Join(output, "summary.json"))).IsTrue();
            var summary = await File.ReadAllTextAsync(Path.Join(output, "summary.json"), cancellationToken);
            _ = await Assert.That(summary).Contains("\"fixedPointReached\":true", StringComparison.Ordinal);
            _ = await Assert.That(summary).Contains("\"violation\":null", StringComparison.Ordinal);
            _ = await Assert.That(summary).Contains(ExploreRunner.ModelVersionHash, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(output, true);
        }
    }

    [Test]
    public async Task SafetyCheckerFormatsCounterexampleJson()
    {
        var state = ClusterState.CreateInitial(3);
        var violation = new SafetyViolation("ElectionSafety", "dual leaders", state.Fingerprint(false));
        var json = SafetyChecker.FormatCounterexampleJson(violation, state, SampleCounterexamplePaths);

        _ = await Assert.That(json).Contains("\"invariant\":\"ElectionSafety\"", StringComparison.Ordinal);
        _ = await Assert.That(json).Contains("\"path\":[\"start\",\"elect\"]", StringComparison.Ordinal);
        _ = await Assert.That(json).Contains("\"nodes\":[", StringComparison.Ordinal);
        _ = await Assert.That(SafetyChecker.Check(state)).IsNull();
    }

    /// <summary>Verifies a run's cleanup removes only the reports this CLI can write, and only the ones this run did not write.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>
    /// Two requirements meet in one directory. A report this run did not write is obsolete, because its name says a rule was
    /// broken while the summary beside it says <c lang="json">"policyViolation":null</c>. And a file this CLI cannot write is
    /// not the CLI's, whatever it is called: matching <c lang="text">policy-*.json</c> deleted a caller's
    /// <c lang="text">policy-user-notes.json</c> on a run that produced no report at all, and the run still exited
    /// successfully. The assertion on the stale report is the one that fails without the cleanup; the assertion on the
    /// foreign file is the one that fails when the cleanup matches a pattern instead of the names it can produce.
    /// </remarks>
    [Test]
    public async Task RunCliCleanupOnlyTouchesItsOwnAsync(CancellationToken cancellationToken)
    {
        const string foreignContent = "policy-user-notes.json — written by the caller, not by this CLI";
        var output = CreateTempDir();
        try
        {
            var foreign = Path.Join(output, "policy-user-notes.json");
            await File.WriteAllTextAsync(foreign, foreignContent, cancellationToken);

            var brokenCode = await ExploreRunner.RunCliAsync("transfer", output, BrokenMode.TransferHint);
            _ = await Assert.That(brokenCode).IsEqualTo(0).Because("the injected fault manifests as a policy violation");
            var staleReport = Path.Join(output, ProtocolPolicyChecker.PolicyReportFileName(ProtocolPolicyChecker.HintSourceRetired));
            _ = await Assert.That(File.Exists(staleReport)).IsTrue().Because("the broken run must leave the report this test then expects to disappear");
            _ = await Assert.That(File.Exists(foreign)).IsTrue()
                .Because("the broken run writes one report and must leave every other file alone");

            var cleanCode = await ExploreRunner.RunCliAsync("transfer", output, BrokenMode.None);
            _ = await Assert.That(cleanCode).IsEqualTo(0);

            var summary = await File.ReadAllTextAsync(Path.Join(output, "summary.json"), cancellationToken);
            _ = await Assert.That(summary).Contains("\"policyViolation\":null", StringComparison.Ordinal);
            _ = await Assert.That(File.Exists(staleReport)).IsFalse()
                .Because("a report this run did not write is obsolete, whichever rule it names");
            _ = await Assert.That(await File.ReadAllTextAsync(foreign, cancellationToken)).IsEqualTo(foreignContent)
                .Because("a name this CLI cannot produce is not its file, and the cleanup may not delete it");
        }
        finally
        {
            Directory.Delete(output, true);
        }
    }

    private static string CreateTempDir()
    {
        var path = Path.Join(Path.GetTempPath(), "squirix-protocol-model-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(path);
        return path;
    }
}
