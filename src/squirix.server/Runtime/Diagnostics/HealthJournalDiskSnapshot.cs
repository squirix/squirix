namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>On-disk journal capacity subsection of health-ready diagnostics.</summary>
/// <param name="State">Current journal disk pressure state label (<c>normal</c>, <c>high</c>, or <c>critical</c>).</param>
/// <param name="MaxBytes">Configured journal total byte cap.</param>
/// <param name="UsedBytes">Current on-disk journal total bytes.</param>
/// <param name="HighWaterBytes">Soft high-water mark bytes (80% of <paramref name="MaxBytes" />).</param>
/// <param name="WriteRejectionActive">Whether durable writes are rejected because usage is at the hard limit.</param>
internal readonly record struct HealthJournalDiskSnapshot(string State, long MaxBytes, long UsedBytes, long HighWaterBytes, bool WriteRejectionActive);
