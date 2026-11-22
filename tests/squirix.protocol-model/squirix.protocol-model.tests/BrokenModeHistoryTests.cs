using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

public sealed class BrokenModeHistoryTests
{
    /// <summary>The modes whose fault the client-history driver models, as the data source passes them to the tests.</summary>
    /// <remarks>
    /// The two transfer-hint modes fault the leader-transfer path, which this driver never takes: it starts a term, writes,
    /// replicates and reads without a planned stop, so a leader that retires and names a successor does not occur in it.
    /// <see cref="CoversEveryBrokenMode" /> keeps this list from swallowing a mode: every member is either named here or
    /// named as a transfer mode, and the two lists together are the whole enum.
    /// </remarks>
    private static readonly string[] ClientHistoryModes =
    [
        nameof(BrokenMode.Vote),
        nameof(BrokenMode.CurrentTermCommit),
        nameof(BrokenMode.ReadIndex),
        nameof(BrokenMode.RepairCeiling),
        nameof(BrokenMode.LogOrder),
    ];

    /// <summary>Returns the modes the client-history driver models.</summary>
    /// <returns><see cref="ClientHistoryModes" />, which the data source reads once per test case.</returns>
    public static string[] Modes() => ClientHistoryModes;

    [Test]
    [MethodDataSource(nameof(Modes))]
    public async Task RejectsBrokenModeClientHistory(string mode, CancellationToken cancellationToken)
    {
        var broken = Enum.Parse<BrokenMode>(mode);
        var scenario = new ClientHistoryScenario(broken);
        _ = await Assert.That(scenario.Run(broken)).IsTrue();
        var history = scenario.History();
        var (writes, reads, _, _) = history.Snapshot();
        _ = await Assert.That(writes.Length).IsEqualTo(1);
        _ = await Assert.That(writes[0].Key).IsEqualTo("target");
        _ = await Assert.That(writes[0].Value).IsEqualTo(1);
        _ = await Assert.That(writes[0].Acked).IsTrue();
        _ = await Assert.That(writes[0].End).IsGreaterThan(writes[0].Start);
        _ = await Assert.That(reads.Length).IsEqualTo(1);
        _ = await Assert.That(reads[0].Key).IsEqualTo("target");
        _ = await Assert.That(reads[0].Observed).IsEqualTo(0);
        _ = await Assert.That(reads[0].Start).IsGreaterThan(writes[0].End);
        _ = await Assert.That(reads[0].End).IsGreaterThan(reads[0].Start);
        _ = await Assert.That(history.Check(cancellationToken)).IsNotEmpty();

        // One model-generated history exercises the file round trip.
        if (broken != BrokenMode.ReadIndex)
            return;

        var path = Path.Join(Path.GetTempPath(), Guid.NewGuid().ToString("N") + ".history.json");
        try
        {
            await RegisterHistoryFile.SaveAsync(path, history, cancellationToken);
            _ = await Assert.That(await RegisterHistoryFile.CheckAsync(path, cancellationToken)).IsNotEmpty();
        }
        finally
        {
            File.Delete(path);
        }
    }

    [Test]
    [MethodDataSource(nameof(Modes))]
    public async Task CorrectRuleBlocksClientAnomaly(string mode)
    {
        var broken = Enum.Parse<BrokenMode>(mode);
        var scenario = new ClientHistoryScenario(BrokenMode.None);
        _ = await Assert.That(scenario.Run(broken)).IsFalse();
        var (writes, reads, _, _) = scenario.History().Snapshot();
        _ = await Assert.That(reads).IsEmpty();
        var stopsBeforeWrite = broken is BrokenMode.Vote or BrokenMode.RepairCeiling;
        _ = await Assert.That(writes.Length).IsEqualTo(stopsBeforeWrite ? 0 : 1);
        if (!stopsBeforeWrite)
            _ = await Assert.That(writes[0].Acked).IsEqualTo(broken != BrokenMode.CurrentTermCommit);
    }

    [Test]
    public async Task AcceptsHealthyModelClientHistory(CancellationToken cancellationToken)
    {
        var scenario = new ClientHistoryScenario(BrokenMode.None);
        _ = await Assert.That(scenario.HealthyRead()).IsTrue();
        var history = scenario.History();
        _ = await Assert.That(history.Snapshot().Reads[0].Observed).IsEqualTo(1);
        _ = await Assert.That(history.Check(cancellationToken)).IsEmpty();
    }

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task ReadIndexWaitsForRecoveryApply(bool broken, CancellationToken cancellationToken)
    {
        var scenario = new ClientHistoryScenario(broken ? BrokenMode.ReadIndex : BrokenMode.None);
        _ = await Assert.That(scenario.ReadBeforeRecoveryApplies()).IsEqualTo(broken);
        var history = scenario.History();
        var (writes, reads, _, _) = history.Snapshot();
        _ = await Assert.That(writes[0].Acked).IsTrue();
        _ = await Assert.That(scenario.State.Nodes[0].AppliedIndex).IsEqualTo(0);
        _ = await Assert.That(reads.Length).IsEqualTo(broken ? 1 : 0);
        if (broken)
        {
            _ = await Assert.That(reads[0].Observed).IsEqualTo(0);
            _ = await Assert.That(reads[0].Start).IsGreaterThan(writes[0].End);
        }

        _ = await Assert.That(history.Check(cancellationToken).Count > 0).IsEqualTo(broken);
    }

    [Test]
    public async Task CoversEveryBrokenMode()
    {
        var transferModes = new[] { nameof(BrokenMode.TransferHint), nameof(BrokenMode.TransferHintStaleAccept) };
        _ = await Assert.That(Enum.GetValues<BrokenMode>().Length).IsEqualTo(8);
        foreach (var mode in Enum.GetValues<BrokenMode>())
            _ = await Assert.That(mode is BrokenMode.None or BrokenMode.Vote or BrokenMode.CurrentTermCommit or BrokenMode.ReadIndex or BrokenMode.RepairCeiling or BrokenMode.LogOrder or BrokenMode.TransferHint or BrokenMode.TransferHintStaleAccept).IsTrue();

        // Every member is either driven by the client-history scenarios or named as a transfer mode, so a mode added later
        // cannot be skipped by both lists and still pass here.
        var notDriven = Array.FindAll(
            Enum.GetNames<BrokenMode>(),
            static name => !string.Equals(name, nameof(BrokenMode.None), StringComparison.Ordinal) && Array.IndexOf(ClientHistoryModes, name) < 0);
        _ = await Assert.That(notDriven.Length).IsEqualTo(transferModes.Length);
        foreach (var transferMode in transferModes)
            _ = await Assert.That(Array.IndexOf(notDriven, transferMode)).IsGreaterThanOrEqualTo(0);
    }
}
