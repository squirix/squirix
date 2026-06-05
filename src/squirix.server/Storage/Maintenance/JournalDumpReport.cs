using System.Collections.Generic;

namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes a read-only journal directory dump.
/// </summary>
/// <param name="FrameCount">The total number of frames.</param>
/// <param name="FirstSequence">The first observed sequence.</param>
/// <param name="LastSequence">The last observed sequence.</param>
/// <param name="Frames">A bounded frame preview.</param>
[JetBrains.Annotations.UsedImplicitly(JetBrains.Annotations.ImplicitUseTargetFlags.WithMembers)]
internal sealed record JournalDumpReport(int FrameCount, ulong FirstSequence, ulong LastSequence, IReadOnlyList<JournalFramePreview> Frames);
