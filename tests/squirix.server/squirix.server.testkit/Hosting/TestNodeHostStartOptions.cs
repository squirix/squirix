using System;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Optional settings for <see cref="TestNodeHostFactory" /> node startup.
/// </summary>
public sealed class TestNodeHostStartOptions
{
    /// <summary>Gets the persistence data directory. When set, the node starts with journal/snapshot persistence enabled.</summary>
    public string? DataDir { get; init; }

    /// <summary>Gets an optional journal segment size override in megabytes when persistence is enabled.</summary>
    public int? JournalMaxSegmentMb { get; init; }

    /// <summary>Gets an optional cap on the number of journal segments when persistence is enabled.</summary>
    public int? JournalMaxSegmentCount { get; init; }

    /// <summary>Gets an optional cap on total journal bytes in megabytes when persistence is enabled.</summary>
    public int? JournalMaxTotalBytesMb { get; init; }

    /// <summary>Gets an optional background flush interval override in milliseconds when persistence is enabled.</summary>
    public int? FlushIntervalMs { get; init; }

    /// <summary>Gets an optional snapshot trigger interval override when persistence is enabled.</summary>
    public TimeSpan? SnapshotInterval { get; init; }

    /// <summary>Gets an optional journal group-commit max wait override in milliseconds when persistence is enabled.</summary>
    public int? JournalGroupCommitMaxWaitMs { get; init; }

    /// <summary>Gets the inter-node mTLS profile for this node in negative-path cluster tests.</summary>
    public TestNodeProfile MtlsProfile { get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets optional per-node security settings.</summary>
    public TestNodeSecurityOptions? Security { get; init; }
}
