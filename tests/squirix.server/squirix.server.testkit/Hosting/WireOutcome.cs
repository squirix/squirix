using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>What a node answered to a <see cref="WireMutation" />.</summary>
/// <param name="Applied">Whether the call took effect: always <see langword="true" /> for a Set, whether the entry was added for a TryAdd or a GetOrAdd.</param>
/// <param name="Value">The value a GetOrAdd returned; <see langword="null" /> for the other mutations.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct WireOutcome(bool Applied, string? Value);
