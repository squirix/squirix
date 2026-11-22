using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>One step the protocol does not allow, which is a different thing from one that endangers the data.</summary>
/// <remarks>
/// A policy violation is kept in its own class so that a run can report "the rules were broken here" without claiming
/// "the data was lost here". A hint emitted by a leader that is still serving reorders terms and breaks no safety
/// invariant, so writing it as <see cref="SafetyViolation" /> would be false; writing it nowhere would leave the fault
/// undetected. The two classes therefore travel in separate fields and separate files.
/// </remarks>
[Immutable]
internal sealed class PolicyViolation
{
    internal PolicyViolation(string policy, string detail, string stateFingerprint, string eventTrace)
    {
        Policy = policy;
        Detail = detail;
        StateFingerprint = stateFingerprint;
        EventTrace = eventTrace;
    }

    /// <summary>Gets the name of the rule that was broken.</summary>
    internal string Policy { get; }

    /// <summary>Gets what was observed, in words that name the nodes and the term.</summary>
    internal string Detail { get; }

    /// <summary>Gets the fingerprint of the whole state in which the step was taken.</summary>
    internal string StateFingerprint { get; }

    /// <summary>Gets the event that broke the rule, rendered so the counterexample can be read without the binaries.</summary>
    internal string EventTrace { get; }
}
