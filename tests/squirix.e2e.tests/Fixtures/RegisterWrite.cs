using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures;

/// <summary>One write of a register history: the value the single writer of the key wrote, and when the call ran.</summary>
/// <param name="Key">The register key.</param>
/// <param name="Value">The value written; values of one key are positive and increase in write order.</param>
/// <param name="Start">When the call started, in <see cref="System.Diagnostics.Stopwatch" /> ticks.</param>
/// <param name="End">When the call returned or failed, in <see cref="System.Diagnostics.Stopwatch" /> ticks.</param>
/// <param name="Acked">
/// <see langword="true" /> when the call returned success; <see langword="false" /> when it failed, so the write may or may not have taken
/// effect.
/// </param>
[Immutable]
internal readonly record struct RegisterWrite(string Key, long Value, long Start, long End, bool Acked);
