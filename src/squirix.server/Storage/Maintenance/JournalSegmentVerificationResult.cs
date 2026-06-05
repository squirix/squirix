namespace Squirix.Server.Storage.Maintenance;

/// <summary>
/// Describes read-only validation of a journal segment.
/// </summary>
/// <param name="IsValid">Whether the segment is valid.</param>
/// <param name="Frames">The number of decoded journal frames.</param>
/// <param name="LastSequence">The largest decoded journal sequence.</param>
/// <param name="Error">The validation failure reason, when available.</param>
[JetBrains.Annotations.UsedImplicitly(JetBrains.Annotations.ImplicitUseTargetFlags.WithMembers)]
internal readonly record struct JournalSegmentVerificationResult(bool IsValid, int Frames, ulong LastSequence, string? Error);
