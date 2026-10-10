namespace Squirix.E2ETests.Fixtures;

/// <summary>The timing evidence of a series of leader stops: the machine, the election timing, every sample and the percentiles.</summary>
/// <param name="Schema">The schema identifier of this file.</param>
/// <param name="Test">The test that recorded it.</param>
/// <param name="Machine">The machine fingerprint of the host.</param>
/// <param name="Os">The description of the operating system.</param>
/// <param name="ProcessorCount">The number of logical processors.</param>
/// <param name="Runtime">The description of the .NET runtime.</param>
/// <param name="ElectionTiming">The election timing of every node.</param>
/// <param name="Gate">Whether the p95 limit was enforced on this host, and why.</param>
/// <param name="Completed">Whether the series ran to the end.</param>
/// <param name="LimitMs">The p95 limit that applies on the controlled machine.</param>
/// <param name="P50Ms">The median recovery time.</param>
/// <param name="P95Ms">The 95th percentile recovery time.</param>
/// <param name="Samples">One entry per leader stop, in order.</param>
internal sealed record FailoverTimingEvidence(
    string Schema,
    string Test,
    string Machine,
    string Os,
    int ProcessorCount,
    string Runtime,
    ElectionTimingEvidence ElectionTiming,
    string Gate,
    bool Completed,
    double LimitMs,
    double P50Ms,
    double P95Ms,
    FailoverSample[] Samples);
