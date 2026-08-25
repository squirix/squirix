using System;
using System.IO;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.ProtocolModel.Tests;

public sealed class ProtocolModelSurfaceTests : ProtocolModelTestBase
{
    private static readonly string[] SampleCounterexamplePaths = ["start", "elect"];

    [Fact]
    public static void ExploreProfileForCliBuildsSmallAndFull()
    {
        var small = ExploreProfile.ForCli("small", true);
        Assert.Equal("small", small.Name, StringComparer.Ordinal);
        Assert.Equal(2, small.Majority);
        Assert.False(small.AllowPartition);

        var full = ExploreProfile.ForCli("full", false);
        Assert.Equal("full", full.Name, StringComparer.Ordinal);
        Assert.True(full.AllowCrash);
        Assert.True(full.AllowPartition);
        Assert.False(full.SymmetryReduce);

        var rf = ExploreProfile.ForReplicaCount(3, 2, 1, 2, 0, true, true);
        Assert.True(rf.AllowPartition);
        Assert.Equal(2, rf.Majority);
    }

    [Fact]
    public static void ExploreProfileForCliRejectsUnknownName() =>
        ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ExploreProfile.ForCli("tiny", true));

    [Fact]
    public static void ExploreReplicaCountProfileRejectsRange()
    {
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ExploreProfile.ForReplicaCount(0, 2, 1, 2, 0, false, true));
        _ = ProtocolModelExceptionAssert.For<ArgumentOutOfRangeException>().Throws(static () => ExploreProfile.ForReplicaCount(33, 2, 1, 2, 0, false, true));
    }

    [Fact]
    public static void LogEntryEqualityMatchesTermAndIndex()
    {
        var a = new LogEntry(1, 2);
        var b = new LogEntry(1, 2);
        var c = new LogEntry(2, 2);
        object boxed = b;
        object other = c;
        object wrong = "x";

        Assert.True(a == b);
        Assert.False(a != b);
        Assert.True(a.Equals(boxed));
        Assert.False(a.Equals(other));
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
        Assert.False(a.Equals(wrong));
    }

    [Fact]
    public static async Task RunCliBrokenVoteCounterexampleAsync()
    {
        var output = CreateTempDir();
        try
        {
            var code = await ExploreRunner.RunCliAsync("small", output, BrokenMode.Vote);
            Assert.Equal(0, code);
            Assert.True(File.Exists(Path.Join(output, "summary.json")));
            Assert.True(File.Exists(Path.Join(output, "counterexample.json")));
            var summary = await File.ReadAllTextAsync(Path.Join(output, "summary.json"), DefaultCancellationToken);
            Assert.Contains("\"broken\":\"Vote\"", summary, StringComparison.Ordinal);
            Assert.Contains("\"invariant\":\"ElectionSafety\"", summary, StringComparison.Ordinal);
            var counterexample = await File.ReadAllTextAsync(Path.Join(output, "counterexample.json"), DefaultCancellationToken);
            Assert.Contains("\"invariant\":\"ElectionSafety\"", counterexample, StringComparison.Ordinal);
            Assert.Contains("\"path\":[", counterexample, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(output, true);
        }
    }

    [Fact]
    public static async Task RunCliFormatsCommitAndReadModesAsync()
    {
        var outputCommit = CreateTempDir();
        var outputRead = CreateTempDir();
        try
        {
            // small profile may not hit these invariants; accept either found (0) or missing (3).
            var commitCode = await ExploreRunner.RunCliAsync("small", outputCommit, BrokenMode.CurrentTermCommit);
            var readCode = await ExploreRunner.RunCliAsync("small", outputRead, BrokenMode.ReadIndex);
            Assert.True(commitCode is 0 or 3);
            Assert.True(readCode is 0 or 3);

            var commitSummary = await File.ReadAllTextAsync(Path.Join(outputCommit, "summary.json"), DefaultCancellationToken);
            var readSummary = await File.ReadAllTextAsync(Path.Join(outputRead, "summary.json"), DefaultCancellationToken);
            Assert.Contains("\"broken\":\"CurrentTermCommit\"", commitSummary, StringComparison.Ordinal);
            Assert.Contains("\"broken\":\"ReadIndex\"", readSummary, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(outputCommit, true);
            Directory.Delete(outputRead, true);
        }
    }

    [Fact]
    public static async Task RunCliWritesSummaryForSmallProfileAsync()
    {
        var output = CreateTempDir();
        try
        {
            var code = await ExploreRunner.RunCliAsync("small", output, BrokenMode.None);
            Assert.Equal(0, code);
            Assert.True(File.Exists(Path.Join(output, "summary.json")));
            var summary = await File.ReadAllTextAsync(Path.Join(output, "summary.json"), DefaultCancellationToken);
            Assert.Contains("\"fixedPointReached\":true", summary, StringComparison.Ordinal);
            Assert.Contains("\"violation\":null", summary, StringComparison.Ordinal);
            Assert.Contains(ExploreRunner.ModelVersionHash, summary, StringComparison.Ordinal);
        }
        finally
        {
            Directory.Delete(output, true);
        }
    }

    [Fact]
    public static void SafetyCheckerFormatsCounterexampleJson()
    {
        var state = ClusterState.CreateInitial(3);
        var violation = new SafetyViolation("ElectionSafety", "dual leaders", state.Fingerprint(false));
        var json = SafetyChecker.FormatCounterexampleJson(violation, state, SampleCounterexamplePaths);

        Assert.Contains("\"invariant\":\"ElectionSafety\"", json, StringComparison.Ordinal);
        Assert.Contains("\"path\":[\"start\",\"elect\"]", json, StringComparison.Ordinal);
        Assert.Contains("\"nodes\":[", json, StringComparison.Ordinal);
        Assert.Null(SafetyChecker.Check(state, BrokenMode.None));
    }

    private static string CreateTempDir()
    {
        var path = Path.Join(Path.GetTempPath(), "squirix-protocol-model-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(path);
        return path;
    }
}
