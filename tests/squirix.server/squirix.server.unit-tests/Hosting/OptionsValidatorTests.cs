using System;
using Microsoft.Extensions.Options;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Validation coverage for hosted option validators registered during node options composition.</summary>
public sealed class OptionsValidatorTests : ServerUnitTestBase
{
    /// <summary>Verifies backpressure validator accepts boundary thresholds at the inclusive limits.</summary>
    [Fact]
    public void BackpressureValidatorAcceptsThresholdBoundaries()
    {
        var v = new AdmissionOptionsValidator();
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
        var v = new AdmissionOptionsValidator();
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
        var v = new AdmissionOptionsValidator();
        var bad = new AdmissionOptions { MaxQueueWait = TimeSpan.Zero };

        var result = v.Validate(Options.DefaultName, bad);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies backpressure validator rejects per-client inflight values above the node cap.</summary>
    [Fact]
    public void BackpressureValidatorRejectsPerFlightAboveNodeCap()
    {
        var v = new AdmissionOptionsValidator();
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
    public void ConfigValidatorAcceptsWellFormedCluster()
    {
        var v = new ConfigValidator();
        var cfg = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") })
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
    public void ConfigValidatorRejectsDuplicatePeerIds()
    {
        var v = new ConfigValidator();
        var cfg = new TopologyOptions(
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
    public void ConfigValidatorRejectsEmptyNodeId()
    {
        var v = new ConfigValidator();
        var cfg = new TopologyOptions(new ServerPeer { NodeId = "x", Uri = new Uri("https://localhost:6001") })
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
    public void ConfigValidatorRejectsInvalidPeerUrls()
    {
        var v = new ConfigValidator();
        var cfg = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("ftp://bad.example/") })
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
    public void ConfigValidatorRejectsPlaintextHttpPeerUrls()
    {
        var v = new ConfigValidator();
        var cfg = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("http://localhost:6001") })
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            VirtualNodes = 128,
        };

        var result = v.Validate(Options.DefaultName, cfg);

        Assert.True(result.Failed);
    }

    /// <summary>Verifies ReplicaCount must be positive and within peer/policy limits.</summary>
    [Fact]
    public void ConfigValidatorRejectsInvalidReplicaCount()
    {
        var v = new ConfigValidator();
        ServerPeer[] peers =
        [
            new() { NodeId = "n1", Uri = new Uri("https://localhost:6001") },
            new() { NodeId = "n2", Uri = new Uri("https://localhost:6002") },
        ];
        var zero = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 0,
        };
        Assert.True(v.Validate(Options.DefaultName, zero).Failed);

        var abovePeers = new TopologyOptions(peers)
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = peers[0].Uri,
            ReplicaCount = 3,
        };
        Assert.True(v.Validate(Options.DefaultName, abovePeers).Failed);
    }

    /// <summary>Verifies ConfigurationGeneration must be greater than zero.</summary>
    [Fact]
    public void ConfigValidatorRejectsZeroConfigurationGeneration()
    {
        var v = new ConfigValidator();
        var cfg = new TopologyOptions(new ServerPeer { NodeId = "n1", Uri = new Uri("https://localhost:6001") })
        {
            ClusterId = "c1",
            NodeId = "n1",
            Uri = new Uri("https://localhost:6001"),
            ConfigurationGeneration = 0,
        };

        Assert.True(v.Validate(Options.DefaultName, cfg).Failed);
    }

    /// <summary>Verifies journal compaction validator accepts valid local scalar values after setter validation.</summary>
    [Fact]
    public void JournalCompactionValidatorAcceptsValidTailSegments()
    {
        var v = new JournalCompactionOptionsValidator();
        var options = new JournalCompactionOptions { MinTailSegments = 0 };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>Verifies journal metrics exporter validator accepts valid intervals after setter validation.</summary>
    [Fact]
    public void JournalMetricsExporterAcceptsValidInterval()
    {
        var v = new JournalMetricsExporterOptionsValidator();
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromTicks(1) };

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }

    /// <summary>Verifies memory pressure cross-property validation stays in the validator path.</summary>
    [Fact]
    public void MemoryPressureValidatorThresholdBelowCritical()
    {
        var v = new PressureOptionsValidator();
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
        var v = new PersistenceOptionsValidator();
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
        var v = new TriggerOptionsValidator();
        var options = new ServerJsonSerializer().Deserialize<TriggerOptions>("""{"snapshotEveryNOps":0}""")!;

        var result = v.Validate(Options.DefaultName, options);

        Assert.False(result.Failed);
    }
}
