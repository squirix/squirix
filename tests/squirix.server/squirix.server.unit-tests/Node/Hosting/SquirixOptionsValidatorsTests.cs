using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Snapshot;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Hosting;

/// <summary>
/// Validation coverage for hosted option validators registered under <see cref="SquirixOptionsValidators" />.
/// </summary>
public sealed class SquirixOptionsValidatorsTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies backpressure validator accepts boundary thresholds at the inclusive limits.
    /// </summary>
    [Fact]
    public void BackpressureValidatorAcceptsInclusiveThresholdBoundaries()
    {
        var v = new SquirixOptionsValidators.BackpressureOptionsValidator();
        var options = new BackpressureOptions
        {
            MaxInFlight = 10,
            SlowdownThreshold = 1,
            RejectThreshold = 10,
            MaxQueue = 0,
            MaxQueueWait = TimeSpan.FromMilliseconds(1),
            MaxSlowdownDelay = TimeSpan.Zero,
        };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>
    /// Verifies a burst without rate limit configuration is rejected.
    /// </summary>
    [Fact]
    public void BackpressureValidatorRejectsBurstWithoutRate()
    {
        var v = new SquirixOptionsValidators.BackpressureOptionsValidator();
        var bad = new BackpressureOptions
        {
            NodeRateLimitBurst = 3,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies backpressure queue wait must remain positive when enabled semantics apply.
    /// </summary>
    [Fact]
    public void BackpressureValidatorRejectsNonPositiveQueueWait()
    {
        var v = new SquirixOptionsValidators.BackpressureOptionsValidator();
        var bad = new BackpressureOptions { MaxQueueWait = TimeSpan.Zero };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies backpressure validator rejects per-client inflight values above the node cap.
    /// </summary>
    [Fact]
    public void BackpressureValidatorRejectsPerClientInFlightAboveNodeCap()
    {
        var v = new SquirixOptionsValidators.BackpressureOptionsValidator();
        var bad = new BackpressureOptions
        {
            MaxInFlight = 8,
            PerClientMaxInFlight = 9,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies a minimal valid cluster configuration passes validation.
    /// </summary>
    [Fact]
    public void ClusterConfigValidatorAcceptsWellFormedCluster()
    {
        var v = new SquirixOptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig
        {
            ClusterId = "c1",
            NodeId = "n1",
            Url = "https://localhost:6001",
            VirtualNodes = 128,
            Peers = [new Peer { NodeId = "n1", Url = "https://localhost:6001" }],
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.False(result.Failed);
    }

    /// <summary>
    /// Verifies duplicate peer identifiers fail validation.
    /// </summary>
    [Fact]
    public void ClusterConfigValidatorRejectsDuplicatePeerIds()
    {
        var v = new SquirixOptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig
        {
            ClusterId = "c1",
            NodeId = "n1",
            Url = "https://localhost:6001",
            VirtualNodes = 128,
            Peers =
            [
                new Peer { NodeId = "n1", Url = "https://localhost:6001" },
                new Peer { NodeId = "n1", Url = "https://localhost:6002" },
            ],
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies empty node identifiers fail validation.
    /// </summary>
    [Fact]
    public void ClusterConfigValidatorRejectsEmptyNodeId()
    {
        var v = new SquirixOptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig
        {
            ClusterId = "c1",
            NodeId = " ",
            Url = "https://localhost:6001",
            VirtualNodes = 128,
            Peers = [new Peer { NodeId = "x", Url = "https://localhost:6001" }],
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies peer URLs must be absolute HTTPS endpoints.
    /// </summary>
    [Fact]
    public void ClusterConfigValidatorRejectsInvalidPeerUrls()
    {
        var v = new SquirixOptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig
        {
            ClusterId = "c1",
            NodeId = "n1",
            Url = "https://localhost:6001",
            VirtualNodes = 128,
            Peers = [new Peer { NodeId = "n1", Url = "ftp://bad.example/" }],
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies plaintext HTTP peer URLs are rejected.
    /// </summary>
    [Fact]
    public void ClusterConfigValidatorRejectsPlaintextHttpPeerUrls()
    {
        var v = new SquirixOptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig
        {
            ClusterId = "c1",
            NodeId = "n1",
            Url = "https://localhost:6001",
            VirtualNodes = 128,
            Peers = [new Peer { NodeId = "n1", Url = "http://localhost:6001" }],
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies memory pressure cross-property validation stays in the validator path.
    /// </summary>
    [Fact]
    public void MemoryPressureValidatorRejectsHighThresholdNotBelowCritical()
    {
        var v = new SquirixOptionsValidators.MemoryPressureOptionsValidator();
        var bad = new MemoryPressureOptions
        {
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 90,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies persistence validation still enforces required paths that cannot be local scalar setter checks.
    /// </summary>
    [Fact]
    public void PersistenceValidatorRejectsEmptyDataDir()
    {
        var v = new SquirixOptionsValidators.PersistenceOptionsValidator();
        var bad = new PersistenceOptions
        {
            DataDir = " ",
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            SnapshotIntervalSec = 5,
            ManifestRetentionCount = 1,
            SnapshotRetentionCount = 1,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>
    /// Verifies snapshot trigger validator accepts valid local scalar values after setter validation.
    /// </summary>
    [Fact]
    public void SnapshotTriggerValidatorAcceptsValidCadence()
    {
        var v = new SquirixOptionsValidators.SnapshotTriggerOptionsValidator();
        var options = new SnapshotTriggerOptions { SnapshotEveryNOps = 0 };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>
    /// Verifies journal compaction validator accepts valid local scalar values after setter validation.
    /// </summary>
    [Fact]
    public void JournalCompactionValidatorAcceptsValidTailSegments()
    {
        var v = new SquirixOptionsValidators.JournalCompactionOptionsValidator();
        var options = new JournalCompactionOptions { MinTailSegments = 0 };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>
    /// Verifies journal metrics exporter validator accepts valid intervals after setter validation.
    /// </summary>
    [Fact]
    public void JournalMetricsExporterValidatorAcceptsValidInterval()
    {
        var v = new SquirixOptionsValidators.JournalMetricsExporterOptionsValidator();
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromTicks(1) };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }
}
