using Squirix.Attributes;

namespace Squirix.E2ETests.Fixtures;

/// <summary>One successful read of a register history: the value seen, and when the call ran.</summary>
/// <param name="Key">The register key.</param>
/// <param name="Start">When the call started, in <see cref="System.Diagnostics.Stopwatch" /> ticks.</param>
/// <param name="End">When the call returned, in <see cref="System.Diagnostics.Stopwatch" /> ticks.</param>
/// <param name="Observed">The value seen; zero when the key was not found, which is the register before its first write.</param>
[Immutable]
internal readonly record struct RegisterRead(string Key, long Start, long End, long Observed);
