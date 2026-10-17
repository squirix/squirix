using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Manifest;

/// <summary>The outcome of reading the two pointer slots.</summary>
/// <param name="ManifestIndex">The manifest index the pointer names; zero when the pointer was never published.</param>
/// <param name="Slot">The slot that holds the selected record; for a never published pointer, the slot the first write must not use.</param>
/// <param name="Sequence">The sequence of the selected record; zero when the pointer was never published.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct PointerSelection(int ManifestIndex, int Slot, ulong Sequence);
