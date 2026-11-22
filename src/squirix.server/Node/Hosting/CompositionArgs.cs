using System;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.Storage;

namespace Squirix.Server.Node.Hosting;

/// <summary>Mutable composition overrides for hosting builder configuration.</summary>
internal sealed class CompositionArgs
{
    internal AdmissionOptions? BackpressureOptions { get; set; }

    internal MtlsCertificate? Certificate { get; set; }

    /// <summary>Gets or sets a value indicating whether the closed replication peer service is mapped for foundation tests.</summary>
    internal bool FoundationOnly { get; set; }

    internal EvictionOptions? Eviction { get; set; }

    internal PressureOptions? MemoryPressureOptions { get; set; }

    internal X509Certificate2? ServerCertificate { get; set; }

    internal Uri? ManagementUri { get; set; }

    internal bool MetricsEnabled { get; set; }

    internal MtlsOptions? MtlsOptions { get; set; }

    internal PersistenceOptions? PersistenceOptions { get; set; }

    internal PrometheusMetricsEndpointOptions? PrometheusMetricsOptions { get; set; }

    internal SecurityOptions? SecurityOptions { get; set; }

    internal bool WaitForRecovery { get; set; } = true;
}
