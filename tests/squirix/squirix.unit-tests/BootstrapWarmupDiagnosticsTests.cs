using System;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Tests bootstrap warm-up skip observability.</summary>
[Immutable]
public sealed class BootstrapWarmupDiagnosticsTests
{
    private const string BootstrapWarmupSkippedInstrumentName = "squirix_client_pool_bootstrap_warmup_skipped_total";

    /// <summary>Verifies skipped bootstrap peers emit a labeled counter measurement.</summary>
    [Fact]
    public void RecordBootstrapPeerSkippedIncrementsMetric()
    {
        using var sink = new MeasurementSink("Squirix");
        ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped("peer-dead", new InvalidOperationException("Failed to connect to endpoint 'peer-dead' within 5000ms."));
        Assert.True(sink.HasEvent(BootstrapWarmupSkippedInstrumentName, ("node_id", "peer-dead"), ("reason", "connect_timeout")));
    }

    /// <summary>Verifies non-timeout failures classify as connect_failed.</summary>
    [Fact]
    public void RecordBootstrapPeerSkippedNonTimeoutFailures()
    {
        using var sink = new MeasurementSink("Squirix");
        ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped("peer-dead", new InvalidOperationException("connection refused"));
        Assert.True(sink.HasEvent(BootstrapWarmupSkippedInstrumentName, ("node_id", "peer-dead"), ("reason", "connect_failed")));
    }
}
