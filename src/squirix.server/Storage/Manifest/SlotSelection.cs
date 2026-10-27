using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Manifest;

/// <summary>The outcome of reading the two manifest slots.</summary>
/// <param name="State">The manifest the newest valid slot holds; the default manifest when the file was never published.</param>
/// <param name="Slot">The slot that holds the selected record; for a never published file, the slot the first write must not use.</param>
/// <param name="Sequence">The sequence of the selected record; zero when the file was never published.</param>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct SlotSelection(State State, int Slot, ulong Sequence);
