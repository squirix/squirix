using System;
using System.Net.Http;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.TestKit.Hosting;

internal sealed class NodeHostStartOptions
{
    internal Action<ILoggingBuilder>? ConfigureLogging { get; init; }

    internal bool WaitForRecovery { get; init; } = true;

    internal TriggerOptions? SnapshotOptions { get; init; }

    internal Func<string, ServerCallPolicy>? CallPolicyFactory { get; init; }

    internal Action<GrpcServiceOptions>? ConfigureGrpc { get; init; }

    internal Action<IServiceCollection>? ServicesConfigure { get; init; }

    internal PersistenceOptions? PersistenceOptions { get; init; }

    internal Func<string, HttpMessageHandler>? PeerHandlerFactory { get; init; }

    internal AdmissionOptions? BackpressureOptions { get; init; }

    internal PressureOptions? MemoryPressureOptions { get; init; }

    internal SecurityOptions? SecurityOptions { get; init; }

    internal Action<ExtensionOptions>? ConfigureExtensions { get; init; }

    internal MtlsOptions? MtlsOptions { get; init; }

    internal MtlsCertificateMaterial? MtlsMaterial { get; init; }
}
