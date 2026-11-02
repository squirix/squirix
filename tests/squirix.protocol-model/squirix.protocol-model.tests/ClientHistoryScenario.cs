namespace Squirix.ProtocolModel.Tests;

internal sealed class ClientHistoryScenario : ClientHistoryDriver
{
    internal ClientHistoryScenario(BrokenMode broken)
        : base(broken)
    {
    }

    internal bool Run(BrokenMode scenario) => scenario switch
    {
        BrokenMode.Vote => DoubleVote(),
        BrokenMode.CurrentTermCommit => EarlierTermCommit(),
        BrokenMode.ReadIndex => StaleLeaderRead(),
        BrokenMode.RepairCeiling => RepairWithoutCeiling(),
        BrokenMode.LogOrder => LengthBeforeTerm(),
        _ => throw new System.ArgumentOutOfRangeException(nameof(scenario)),
    };

    internal bool HealthyRead()
    {
        Bootstrap();
        Write(0);
        Replicate(0, 1, 2);
        Apply(0);
        Require(Acknowledge(0), "healthy write committed");
        return Read(0, 1);
    }

    internal bool ReadBeforeRecoveryApplies()
    {
        Bootstrap();
        Write(0);
        Replicate(0, 1, 2);
        Apply(0);
        Require(Acknowledge(0), "write acknowledged before restart");
        DropAll();
        Crash(0);
        Require(Elect(0, 1), "restarted leader before replay");
        DropAll();
        return Read(0, 1);
    }

    private bool DoubleVote()
    {
        Require(Elect(0, 2), "first election");
        DropAll();
        if (!Elect(1, 2))
            return false;
        DropAll();
        Replicate(0, 2, 1);
        Propose(1);
        Replicate(1, 2, 2);
        Apply(1);
        Propose(0);
        Write(0);
        Replicate(0, 2, 3);
        Apply(0);
        Require(Acknowledge(0), "write committed by first leader");
        return Read(1, 2);
    }

    private bool RepairWithoutCeiling()
    {
        Bootstrap();
        Require(Elect(2, 1), "higher-term election");
        DropAll();
        Step("lose voter disk", static next => next.Nodes[1].Unseeded);
        if (!TryStep("repair from old leader", static next => !next.Nodes[1].Unseeded && next.Nodes[1].CurrentTerm == 1))
            return false;
        Require(Elect(1, 0), "second leader in repaired term");
        DropAll();
        Replicate(1, 0, 2);
        Replicate(2, 0, 2);
        Apply(1);
        Write(2);
        Replicate(2, 0, 3);
        Apply(2);
        Require(Acknowledge(2), "write committed before stale read");
        return Read(1, 0);
    }

    private bool StaleLeaderRead()
    {
        Bootstrap();
        Require(Elect(2, 1), "new leader without informing old leader");
        DropAll();
        Replicate(2, 1, 2);
        Write(2);
        Replicate(2, 1, 3);
        Apply(2);
        Require(Acknowledge(2), "new leader write");
        return Read(0, 1);
    }

    private bool EarlierTermCommit()
    {
        Bootstrap();
        Write(0);
        DropAll();
        Require(Elect(2, 1), "intervening leader");
        DropAll();
        Crash(2);
        Crash(0);
        Campaign(0);
        DropAll();
        Require(Elect(0, 1), "leader retaining earlier-term client entry");
        DropAll();
        Replicate(0, 1, 2);
        Apply(0);
        if (!Acknowledge(0))
            return false;
        DropAll();
        Campaign(2);
        DropAll();
        Require(Elect(2, 1), "later leader missing acknowledged entry");
        DropAll();
        Replicate(2, 1, 3);
        Apply(2);
        return Read(2, 1);
    }

    private bool LengthBeforeTerm()
    {
        Bootstrap();
        Propose(0);
        Propose(0);
        Propose(0);
        DropAll();
        Require(Elect(2, 1), "shorter newer-term log");
        DropAll();
        Replicate(2, 1, 2);
        Write(2);
        Replicate(2, 1, 3);
        Apply(2);
        Require(Acknowledge(2), "newer-term client write");
        DropAll();
        Crash(0);
        Campaign(0);
        DropAll();
        if (!Elect(0, 1))
            return false;
        DropAll();
        Apply(0);
        return Read(0, 1);
    }
}
