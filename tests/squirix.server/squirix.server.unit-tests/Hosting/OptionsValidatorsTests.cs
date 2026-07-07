using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>
/// Validation coverage for hosted option validators registered under <see cref="OptionsValidators" />.
/// </summary>
public sealed class OptionsValidatorsTests : UnitTestBase
{
    /// <summary>Verifies backpressure validator accepts boundary thresholds at the inclusive limits.</summary>
    [Fact]
    public void BackpressureValidatorAcceptsInclusiveThresholdBoundaries()
    {
        var v = new OptionsValidators.BackpressureOptionsValidator();
        var options = new AdmissionOptions
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

    /// <summary>Verifies a burst without rate limit configuration is rejected.</summary>
    [Fact]
    public void BackpressureValidatorRejectsBurstWithoutRate()
    {
        var v = new OptionsValidators.BackpressureOptionsValidator();
        var bad = new AdmissionOptions
        {
            NodeRateLimitBurst = 3,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies backpressure queue wait must remain positive when enabled semantics apply.</summary>
    [Fact]
    public void BackpressureValidatorRejectsNonPositiveQueueWait()
    {
        var v = new OptionsValidators.BackpressureOptionsValidator();
        var bad = new AdmissionOptions { MaxQueueWait = TimeSpan.Zero };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies backpressure validator rejects per-client inflight values above the node cap.</summary>
    [Fact]
    public void BackpressureValidatorRejectsPerClientInFlightAboveNodeCap()
    {
        var v = new OptionsValidators.BackpressureOptionsValidator();
        var bad = new AdmissionOptions
        {
            MaxInFlight = 8,
            PerClientMaxInFlight = 9,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies a minimal valid cluster configuration passes validation.</summary>
    [Fact]
    public void ClusterConfigValidatorAcceptsWellFormedCluster()
    {
        var v = new OptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig([new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") }])
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            VirtualNodes = 128,
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.False(result.Failed);
    }

    /// <summary>Verifies duplicate peer identifiers fail validation.</summary>
    [Fact]
    public void ClusterConfigValidatorRejectsDuplicatePeerIds()
    {
        var v = new OptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig(
            [
                new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") },
                new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6002") },
            ])
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            VirtualNodes = 128,
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies empty node identifiers fail validation.</summary>
    [Fact]
    public void ClusterConfigValidatorRejectsEmptyNodeId()
    {
        var v = new OptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig([new ServerPeer { NodeId = "x", Uri = new Uri("https://localhost:6001") }])
        {
            ClusterId = "c1",
            NodeId = " ",
            Uri = new Uri("https://localhost:6001"),
            VirtualNodes = 128,
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies peer URLs must be absolute HTTPS endpoints.</summary>
    [Fact]
    public void ClusterConfigValidatorRejectsInvalidPeerUrls()
    {
        var v = new OptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig([new ServerPeer { NodeId = "n1", Uri = new Uri("ftp://bad.example/") }])
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            VirtualNodes = 128,
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies plaintext HTTP peer URLs are rejected.</summary>
    [Fact]
    public void ClusterConfigValidatorRejectsPlaintextHttpPeerUrls()
    {
        var v = new OptionsValidators.ClusterConfigValidator();
        var cfg = new ClusterConfig([new ServerPeer { NodeId = "n1", Uri = new Uri("http://localhost:6001") }])
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            VirtualNodes = 128,
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies journal compaction validator accepts valid local scalar values after setter validation.</summary>
    [Fact]
    public void JournalCompactionValidatorAcceptsValidTailSegments()
    {
        var v = new OptionsValidators.JournalCompactionOptionsValidator();
        var options = new JournalCompactionOptions { MinTailSegments = 0 };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>Verifies journal metrics exporter validator accepts valid intervals after setter validation.</summary>
    [Fact]
    public void JournalMetricsExporterValidatorAcceptsValidInterval()
    {
        var v = new OptionsValidators.JournalMetricsExporterOptionsValidator();
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromTicks(1) };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>Verifies memory pressure cross-property validation stays in the validator path.</summary>
    [Fact]
    public void MemoryPressureValidatorRejectsHighThresholdNotBelowCritical()
    {
        var v = new OptionsValidators.MemoryPressureOptionsValidator();
        var bad = new PressureOptions
        {
            MaxEstimatedCacheBytes = 1024,
            HighPressureThresholdPercent = 90,
            CriticalPressureThresholdPercent = 90,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies persistence validation still enforces required paths that cannot be local scalar setter checks.</summary>
    [Fact]
    public void PersistenceValidatorRejectsEmptyDataDir()
    {
        var v = new OptionsValidators.PersistenceOptionsValidator();
        var bad = new PersistenceOptions
        {
            DataDir = " ",
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 5,
            ManifestRetentionCount = 1,
            SnapshotRetentionCount = 1,
        };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies snapshot trigger validator accepts valid local scalar values after setter validation.</summary>
    [Fact]
    public void SnapshotTriggerValidatorAcceptsValidCadence()
    {
        var v = new OptionsValidators.SnapshotTriggerOptionsValidator();
        var options = new TriggerOptions { SnapshotEveryNOps = 0 };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }
}
