using System.Collections.Generic;
using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Explores the voter-local pre-vote decision independently of the log and majority election model.</summary>
/// <remarks>
/// One source, successor and voter stand for distinct authenticated members. The queue holds one ordinary or
/// forwarded pre-vote. The voter can expire contact, accept a new leader in the next term, or install the
/// next configuration with that request in flight. An ordinary grant still needs the log check in the server.
/// A bypass records only the decision to waive contact, before the log check. No transition grants authority.
/// </remarks>
public sealed class TransferPreVoteScopeTests
{
    private enum Request
    {
        None = 0,
        Ordinary = 1,
        Transfer = 2,
    }

    /// <summary>Every interleaving permits scoped progress while confining the contact exception to its original context.</summary>
    [Test]
    public async Task ContactBypassMatchesRetiredContext()
    {
        var result = Explore(false);
        _ = await Assert.That(result.Violation).IsNull();
        _ = await Assert.That(result.Bypasses > 0).IsTrue();
        _ = await Assert.That(result.Refusals > 0).IsTrue();
    }

    /// <summary>A forwarded claim alone lets an old request waive contact after the voter changes context.</summary>
    [Test]
    public async Task UnscopedBypassHasCounterexample()
    {
        var result = Explore(true);
        _ = await Assert.That(result.Violation).IsNotNull();
        _ = await Assert.That(result.Violation).Contains("retire");
        _ = await Assert.That(result.Violation).Contains("accept hint");
        _ = await Assert.That(result.Violation).Contains("send transfer pre-vote");
        _ = await Assert.That(result.Violation).Contains("waive contact");
    }

    private static Exploration Explore(bool ignoreScope)
    {
        var initial = new State(false, false, false, true, true, true, Request.None);
        var queue = new Queue<(State State, string Path)>();
        var seen = new HashSet<State> { initial };
        var bypasses = 0;
        var refusals = 0;
        queue.Enqueue((initial, "source leading"));
        while (queue.TryDequeue(out var item))
        {
            var (state, path) = item;
            var granted = GrantsBypass(state, ignoreScope);
            if (granted)
                bypasses++;
            else if (state.Pending != Request.None && state.Recent)
                refusals++;

            if (granted && !LegalBypass(state))
                return new Exploration(bypasses, refusals, path + " -> waive contact");

            foreach (var step in Steps(state))
            {
                if (seen.Add(step.State))
                    queue.Enqueue((step.State, path + " -> " + step.Event));
            }
        }

        return new Exploration(bypasses, refusals, null);
    }

    private static bool GrantsBypass(State state, bool ignoreScope) =>
        state.Recent && state.Pending == Request.Transfer && (ignoreScope || (state.SourceTerm && state.ConfigurationMatches));

    private static bool LegalBypass(State state) =>
        state.Retired && state.Accepted && state.SourceTerm && state.ConfigurationMatches;

    private static IEnumerable<(State State, string Event)> Steps(State state)
    {
        if (!state.Retired)
            yield return (state with { Retired = true }, "retire");
        if (state.Retired && !state.Accepted && state.TargetReady)
            yield return (state with { Accepted = true }, "accept hint");
        yield return (state with { TargetReady = !state.TargetReady }, "change target readiness");
        if (!state.Retired && state.SourceTerm)
            yield return (state with { Recent = true }, "refresh source contact");
        if (state.Recent)
            yield return (state with { Recent = false }, "expire voter contact");
        yield return (state with { SourceTerm = false, Recent = true }, "hear higher-term leader");
        yield return (state with { ConfigurationMatches = false }, "install new configuration");
        if (state.Pending == Request.None)
        {
            yield return (state with { Pending = Request.Ordinary }, "send ordinary pre-vote");
            if (state.Accepted && state.TargetReady)
                yield return (state with { Pending = Request.Transfer }, "send transfer pre-vote");
        }
        else
        {
            yield return (state with { Pending = Request.None }, "deliver or drop");
        }
    }

    private readonly record struct State(bool Retired, bool Accepted, bool TargetReady, bool SourceTerm, bool ConfigurationMatches, bool Recent, Request Pending);

    private readonly record struct Exploration(int Bypasses, int Refusals, string? Violation);
}
