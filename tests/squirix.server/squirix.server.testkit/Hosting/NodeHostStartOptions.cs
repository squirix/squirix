using System;
using System.Net.Http;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;

namespace Squirix.Server.TestKit.Hosting;

[Immutable]
internal sealed class NodeHostStartOptions
{
    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    internal Action<ILoggingBuilder>? ConfigureLogging { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }

    internal MtlsCertificateMaterial? MtlsMaterial { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    /// <summary>Gets a value indicating whether the closed replication service is mapped for transport/identity tests only.</summary>
    internal bool FoundationOnly { get; init; }

    internal Func<string, HttpMessageHandler>? PeerHandlerFactory { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal SecurityOptions? SecurityOptions { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal bool WaitForRecovery { get; init; } = true;
}
