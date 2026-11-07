using System.Collections.Immutable;

namespace Squirix.Testing;

/// <summary>Immutable results of the three independent logger architecture rules.</summary>
/// <param name="StaticMembers">Static fields, properties and methods holding or returning loggers.</param>
/// <param name="StaticClassParameters">Static class methods accepting logger parameters.</param>
/// <param name="OptionalLoggers">Optional, nullable or settable logger parameters and members.</param>
public sealed record LoggerScanResult(
    ImmutableArray<string> StaticMembers,
    ImmutableArray<string> StaticClassParameters,
    ImmutableArray<string> OptionalLoggers);
