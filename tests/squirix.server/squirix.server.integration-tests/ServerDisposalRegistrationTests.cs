using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// General regression guard for issue 461. MS DI does not dispose IAsyncDisposable singletons that are registered
/// through the instance overloads of AddSingleton (for example AddSingleton(runtime)). Such a service must be
/// registered through the factory overload so the host owns its disposal on shutdown. This rule scans the entire
/// server composition (not just persistence) and fails for any IAsyncDisposable-only service registered via the
/// instance form, so future regressions of this class are caught wherever they appear.
/// </summary>
public sealed class ServerDisposalRegistrationTests
{
    /// <summary>Every IAsyncDisposable-only server singleton must be registered via the factory overload.</summary>
    [Fact]
    public async Task AsyncDisposableRegistrationsUseFactory()
    {
        var dataDir = Path.Join(Path.GetTempPath(), "squirix-disposal-rule-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(dataDir);

        var uri = new Uri("https://127.0.0.1:6001");
        const string nodeId = "node-disposal";
        var cluster = new TopologyOptions([new ServerPeer { NodeId = nodeId, Uri = uri }])
        {
            NodeId = nodeId,
            Uri = uri,
            VirtualNodes = 128,
            ReplicaCount = 1,
        };

        var mtlsOptions = new MtlsOptions();
        var mtlsMaterial = MtlsCertificateMaterial.Load(mtlsOptions, null, false);
        var persistenceOptions = new PersistenceOptions { DataDir = dataDir };

        var offenders = new List<string>(64);
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, null));
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, static args => args.FoundationOnly = true));
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, static args => args.SecurityOptions = new SecurityOptions()));
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, static args => args.Extensions = new ExtensionOptions()));

        Assert.Empty(offenders);
    }

    private static async Task<List<string>> ScanAsync(
        TopologyOptions cluster,
        MtlsCertificateMaterial mtlsMaterial,
        MtlsOptions mtlsOptions,
        PersistenceOptions persistenceOptions,
        Action<ICompositionArgs>? extra)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions
        {
            Args = [],
            ApplicationName = "Squirix.Server",
        });

        Action<ICompositionArgs> configure = args =>
        {
            args.MtlsMaterial = mtlsMaterial;
            args.MtlsOptions = mtlsOptions;
            args.PersistenceOptions = persistenceOptions;
            args.WaitForRecovery = false;
            extra?.Invoke(args);
        };

        await ServerHostingComposition.ConfigureBuilderAsync(builder, cluster, configure, Xunit.TestContext.Current.CancellationToken);

        var offenders = new List<string>(builder.Services.Count);
        foreach (var descriptor in builder.Services)
        {
            if (descriptor.ImplementationInstance is { } instance && instance is IAsyncDisposable && instance is not IDisposable)
                offenders.Add(descriptor.ServiceType.FullName ?? descriptor.ServiceType.Name);
        }

        return offenders;
    }
}
