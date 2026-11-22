namespace Squirix.E2ETests.Fixtures;

/// <summary>A peer call observed during client recovery, relative to the start of the stop.</summary>
/// <param name="ElapsedMs">Milliseconds since the stop began.</param>
/// <param name="Sender">The sending member.</param>
/// <param name="Target">The receiving member.</param>
/// <param name="Rpc">The election operation.</param>
/// <param name="Term">The term carried by the operation.</param>
internal sealed record ElectionCall(double ElapsedMs, string Sender, string Target, string Rpc, ulong Term);
