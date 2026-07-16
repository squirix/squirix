using System;
using System.Net.Http;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Hosting;

/// <summary>Optional overrides for <see cref="ServerHostingComposition" /> configuration.</summary>
internal sealed class CompositionArgs
{
    internal bool WaitForRecovery { get; init; }

    internal TriggerOptions? SnapshotOptions { get; init; }

    internal Func<string, ServerCallPolicy>? CallPolicyFactory { get; init; }

    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal Func<string, HttpMessageHandler>? PeerHandlerFactory { get; init; }

    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }

    internal SecurityOptions? SecurityOptions { get; init; }

    internal SquirixServerExtensionOptions? Extensions { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    internal MtlsCertificateMaterial? MtlsMaterial { get; init; }
}
