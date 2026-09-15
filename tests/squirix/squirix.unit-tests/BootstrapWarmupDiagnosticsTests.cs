using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Tests bootstrap warm-up skip observability.</summary>
[Immutable]
public sealed class BootstrapWarmupDiagnosticsTests
{
    private const string BootstrapWarmupSkippedInstrumentName = "squirix_client_pool_bootstrap_warmup_skipped_total";

    /// <summary>Verifies skipped bootstrap peers emit a labeled counter measurement.</summary>
    [Test]
    public async Task RecordPeerSkippedIncrementsMetric()
    {
        using var sink = new MeasurementSink("Squirix");
        ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped("peer-dead", new InvalidOperationException("Failed to connect to endpoint 'peer-dead' within 5000ms."));
        _ = await Assert.That(sink.HasEvent(BootstrapWarmupSkippedInstrumentName, ("node_id", "peer-dead"), ("reason", "connect_timeout"))).IsTrue();
    }

    /// <summary>Verifies non-timeout failures classify as connect_failed.</summary>
    [Test]
    public async Task RecordPeerSkippedNonTimeoutFailures()
    {
        using var sink = new MeasurementSink("Squirix");
        ClientPoolBootstrapWarmupDiagnostics.RecordBootstrapPeerSkipped("peer-dead", new InvalidOperationException("connection refused"));
        _ = await Assert.That(sink.HasEvent(BootstrapWarmupSkippedInstrumentName, ("node_id", "peer-dead"), ("reason", "connect_failed"))).IsTrue();
    }
}
