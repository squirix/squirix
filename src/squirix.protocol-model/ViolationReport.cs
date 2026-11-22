using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

/// <summary>The two kinds of finding one exploration can carry, kept apart in one value.</summary>
/// <param name="Safety">The data-safety violation, when the run found one.</param>
/// <param name="SafetyState">The state in which the data-safety violation was found.</param>
/// <param name="Policy">The first step the protocol does not allow, when the run took one.</param>
/// <param name="PolicyState">The state that step produced.</param>
/// <remarks>
/// They are grouped rather than merged: a safety violation says the data is endangered and stops the search, a policy
/// violation says the step was not the protocol's and does not. They travel together only because they are both findings
/// about the same run.
/// </remarks>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct ViolationReport(SafetyViolation? Safety, ClusterState? SafetyState, PolicyViolation? Policy, ClusterState? PolicyState)
{
    /// <summary>Gets a report with no finding at all.</summary>
    internal static ViolationReport None => default;
}
