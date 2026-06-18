using System;
using System.Globalization;

namespace Squirix.E2ETests.Support.Stress;

/// <summary>
/// Environment-driven targets for journal volume stress. Dial intensity with <c>SQUIRIX_JOURNAL_STRESS_MB</c>,
/// <c>SQUIRIX_JOURNAL_STRESS_GB</c>, or <c>SQUIRIX_JOURNAL_SEGMENT_MB</c> without recompiling.
/// </summary>
internal static class JournalVolumeStressSettings
{
    private const string TargetMbVariable = "SQUIRIX_JOURNAL_STRESS_MB";
    private const string TargetGbVariable = "SQUIRIX_JOURNAL_STRESS_GB";
    private const string SegmentMbVariable = "SQUIRIX_JOURNAL_SEGMENT_MB";
    private const string RpcPerAttemptTimeoutSecVariable = "SQUIRIX_RPC_PER_ATTEMPT_TIMEOUT_SEC";
    private const string PersistDirVariable = "SQUIRIX_JOURNAL_STRESS_PERSIST_DIR";

    /// <summary>Gets optional SDK per-attempt RPC timeout override for journal volume stress.</summary>
    internal static TimeSpan? RpcPerAttemptTimeout { get; } = ResolveRpcPerAttemptTimeout();

    /// <summary>Gets optional fixed persistence directory preserved for post-failure inspection.</summary>
    internal static string? PersistDir { get; } = ResolvePersistDir();

    /// <summary>Gets the concurrent writer count for the journal volume workload.</summary>
    internal static int WriterCount => 4;

    /// <summary>Gets the payload size written for each cache entry.</summary>
    internal static int PayloadBytes => 32 * 1024;

    /// <summary>Gets the number of keys randomly sampled after each restart.</summary>
    internal static int SampleCount => 500;

    /// <summary>Gets the hard deadline for the full journal volume scenario.</summary>
    internal static TimeSpan Budget { get; } = ResolveBudget();

    /// <summary>Gets the target on-disk journal byte volume to reach before restart phases.</summary>
    internal static long TargetJournalBytes { get; } = ResolveTargetJournalBytes();

    /// <summary>Gets the journal segment size override in megabytes.</summary>
    internal static int SegmentMegabytes { get; } = ResolveSegmentMegabytes();

    /// <summary>
    /// Gets the journal segment-count cap sized to hold the full target volume on disk without
    /// triggering the server's capacity guard. The scenario deliberately lets the journal grow to
    /// <see cref="TargetJournalBytes" /> (asserting the on-disk peak) instead of relying on compaction,
    /// so the cap must exceed the segments required for the target plus headroom.
    /// </summary>
    internal static int JournalMaxSegmentCount { get; } = ResolveSegmentCountCap();

    /// <summary>Gets the total-journal-bytes cap in megabytes sized to hold the full target volume.</summary>
    internal static int JournalMaxTotalBytesMb { get; } = ResolveTotalBytesMbCap();

    private static TimeSpan ResolveBudget()
    {
#if DEBUG
        return TimeSpan.FromMinutes(10);
#else
        return TimeSpan.FromMinutes(30);
#endif
    }

    private static long ResolveTargetJournalBytes()
    {
        var gbRaw = Environment.GetEnvironmentVariable(TargetGbVariable);
        if (!string.IsNullOrWhiteSpace(gbRaw) && double.TryParse(gbRaw, NumberStyles.Float, CultureInfo.InvariantCulture, out var gigabytes) && gigabytes > 0d)
        {
            return Convert.ToInt64(gigabytes * 1024 * 1024 * 1024, CultureInfo.InvariantCulture);
        }

        var mbRaw = Environment.GetEnvironmentVariable(TargetMbVariable);
        if (!string.IsNullOrWhiteSpace(mbRaw) && double.TryParse(mbRaw, NumberStyles.Float, CultureInfo.InvariantCulture, out var megabytes) && megabytes > 0d)
        {
            return Convert.ToInt64(megabytes * 1024d * 1024d, CultureInfo.InvariantCulture);
        }

#if DEBUG
        return 32L * 1024L * 1024L;
#else
        return 64L * 1024L * 1024L;
#endif
    }

    private static int ResolveSegmentMegabytes()
    {
        const int hardMaxSegmentCount = 1024;
        var minimum = ComputeMinimumSegmentMegabytesForTarget(hardMaxSegmentCount);
        var fromEnv = TryParseEnvSegmentMb();
        if (fromEnv is { } configured)
            return Math.Max(configured, minimum);

        return minimum;
    }

    private static int? TryParseEnvSegmentMb()
    {
        var raw = Environment.GetEnvironmentVariable(SegmentMbVariable);
        if (!string.IsNullOrWhiteSpace(raw) && int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed) && parsed > 0)
        {
            return parsed;
        }

        return null;
    }

    /// <summary>
    /// Smallest segment size (MB) so the stress target fits under the server hard segment-count cap with roll headroom.
    /// A 1 GB target at 1 MB segments needs about 1024 files and trips journal capacity on the next roll.
    /// </summary>
    /// <param name="hardMaxSegmentCount">Server hard cap on journal segment files.</param>
    private static int ComputeMinimumSegmentMegabytesForTarget(int hardMaxSegmentCount)
    {
        var targetMb = TargetJournalBytes / (1024.0 * 1024.0);
        if (targetMb <= 0d)
            return 1;

        for (var segmentMb = 1; segmentMb <= 4096; segmentMb = segmentMb < 64 ? segmentMb * 2 : segmentMb + 64)
        {
            var needed = Convert.ToInt32(Math.Ceiling(targetMb / segmentMb));
            var cappedCount = Math.Min(needed + Math.Max(16, needed / 4), hardMaxSegmentCount);
            if (needed <= cappedCount && needed + 1 < hardMaxSegmentCount)
                return segmentMb;
        }

        return 64;
    }

    private static int ResolveSegmentCountCap()
    {
        // Server hard cap is 1024 segments (JournalSegmentLimits.HardMaxSegmentCount); values above it
        // are clamped down server-side, so a 1 MB-segment journal cannot exceed ~1 GB regardless.
        const int hardMaxSegmentCount = 1024;
        var segmentBytes = Convert.ToInt64(SegmentMegabytes) * 1024L * 1024L;
        var needed = Convert.ToInt32(Math.Ceiling(TargetJournalBytes / Convert.ToDouble(segmentBytes)));
        var cap = needed + Math.Max(16, needed / 4);
        return Math.Clamp(cap, 4, hardMaxSegmentCount);
    }

    private static int ResolveTotalBytesMbCap()
    {
        // Server hard cap is 65536 MB (JournalSegmentLimits.HardMaxTotalBytesMb).
        const int hardMaxTotalBytesMb = 65536;
        var targetMb = Convert.ToInt32(Math.Ceiling(TargetJournalBytes / (1024d * 1024d)));
        var cap = (targetMb * 2) + 64;
        return Math.Clamp(cap, 1, hardMaxTotalBytesMb);
    }

    private static string? ResolvePersistDir()
    {
        var raw = Environment.GetEnvironmentVariable(PersistDirVariable);
        return string.IsNullOrWhiteSpace(raw) ? null : raw;
    }

    private static TimeSpan? ResolveRpcPerAttemptTimeout()
    {
        var raw = Environment.GetEnvironmentVariable(RpcPerAttemptTimeoutSecVariable);
        if (!string.IsNullOrWhiteSpace(raw) && int.TryParse(raw, NumberStyles.Integer, CultureInfo.InvariantCulture, out var seconds) && seconds > 0)
        {
            return TimeSpan.FromSeconds(seconds);
        }

        return null;
    }
}
