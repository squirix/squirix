using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[Immutable]
internal sealed class SafetyViolation
{
    internal SafetyViolation(string invariant, string detail, string stateFingerprint)
    {
        Invariant = invariant;
        Detail = detail;
        StateFingerprint = stateFingerprint;
    }

    internal string Invariant { get; }

    internal string Detail { get; }

    internal string StateFingerprint { get; }
}
