using System.Runtime.InteropServices;

namespace Squirix.E2ETests.Fixtures;

/// <summary>One step of a <see cref="FaultSchedule" />.</summary>
/// <param name="Kind">What happens.</param>
/// <param name="NodeId">The node it happens to; empty for <see cref="FaultKind.Heal" />, which restores every link.</param>
[StructLayout(LayoutKind.Auto)]
internal readonly record struct FaultStep(FaultKind Kind, string NodeId);
