using System;
using System.Net.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
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

    internal ExtensionOptions? Extensions { get; set; }

    /// <summary>Gets or sets a value indicating whether the closed replication peer service is mapped for foundation tests.</summary>
    internal bool FoundationOnly { get; set; }

    internal PressureOptions? MemoryPressureOptions { get; set; }

    internal bool MetricsEnabled { get; set; }

    internal MtlsOptions? MtlsOptions { get; set; }

    internal Func<string, HttpMessageHandler>? PeerHandlerFactory { get; set; }

    internal PersistenceOptions? PersistenceOptions { get; set; }

    internal PrometheusMetricsEndpointOptions? PrometheusMetricsOptions { get; set; }

    internal SecurityOptions? SecurityOptions { get; set; }

    internal Action<IServiceCollection>? ServicesConfigure { get; set; }

    internal bool WaitForRecovery { get; set; } = true;
}
