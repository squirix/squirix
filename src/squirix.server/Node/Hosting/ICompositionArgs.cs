using System;
using System.Net.Http;
using Grpc.AspNetCore.Server;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;

namespace Squirix.Server.Node.Hosting;

/// <summary>Mutable composition overrides for hosting builder configuration.</summary>
internal interface ICompositionArgs
{
    AdmissionOptions? BackpressureOptions { get; set; }

    Action<GrpcServiceOptions>? ConfigureGrpc { get; set; }

    ExtensionOptions? Extensions { get; set; }

    PressureOptions? MemoryPressureOptions { get; set; }

    MtlsCertificateMaterial? MtlsMaterial { get; set; }

    MtlsOptions? MtlsOptions { get; set; }

    Func<string, HttpMessageHandler>? PeerHandlerFactory { get; set; }

    PersistenceOptions? PersistenceOptions { get; set; }

    SecurityOptions? SecurityOptions { get; set; }

    Action<IServiceCollection>? ServicesConfigure { get; set; }

    bool WaitForRecovery { get; set; }
}
