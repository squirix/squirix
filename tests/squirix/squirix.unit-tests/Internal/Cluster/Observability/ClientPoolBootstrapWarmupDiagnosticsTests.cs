using System;
using Squirix.Internal.Cluster.Observability;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests.Internal.Cluster.Observability;

/// <summary>
/// Tests bootstrap warm-up skip observability.
/// </summary>
public sealed class ClientPoolBootstrapWarmupDiagnosticsTests
{
    private const string BootstrapWarmupSkippedInstrumentName = "squirix_client_pool_bootstrap_warmup_skipped_total";

    /// <summary>
    /// Verifies non-timeout failures classify as connect_failed.
    /// </summary>
    [Fact]
    public void RecordBootstrapPeerSkippedClassifiesNonTimeoutFailures()
    {
        using var sink = new MeasurementSink("Squirix");
        ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped("peer-dead", new InvalidOperationException("connection refused"));
        Assert.True(sink.HasEvent(BootstrapWarmupSkippedInstrumentName, ("node_id", "peer-dead"), ("reason", "connect_failed")));
    }

    /// <summary>
    /// Verifies skipped bootstrap peers emit a labeled counter measurement.
    /// </summary>
    [Fact]
    public void RecordBootstrapPeerSkippedIncrementsSkippedTotalMetric()
    {
        using var sink = new MeasurementSink("Squirix");
        ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped("peer-dead", new InvalidOperationException("Failed to connect to endpoint 'peer-dead' within 5000ms."));
        Assert.True(sink.HasEvent(BootstrapWarmupSkippedInstrumentName, ("node_id", "peer-dead"), ("reason", "connect_timeout")));
    }
}
