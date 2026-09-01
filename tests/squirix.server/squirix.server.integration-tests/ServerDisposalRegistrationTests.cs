using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// General regression guard for issue 461. MS DI does not dispose IDisposable/IAsyncDisposable singletons that are
/// registered through the instance overloads of AddSingleton (for example, AddSingleton(runtime)). Such a service
/// must be registered through the factory overload so the host owns its disposal on shutdown. This rule scans the
/// server composition (not just persistence) and fails for any disposable service that the composition registers via
/// the instance form, so future regressions of this class (including Meters) are caught wherever they appear.
/// </summary>
public sealed class ServerDisposalRegistrationTests : NodeIntegrationTestBase
{
    /// <summary>Every disposable server singleton must be registered via the factory overload.</summary>
    [Fact]
    public async Task AsyncDisposableRegistrationsUseFactory()
    {
        using var dir = new TempDirectory("squirix-follower-restart-committed");

        var uri = new Uri("https://127.0.0.1:6001");
        const string nodeId = "node-disposal";

        var mtlsOptions = new MtlsOptions();
        var mtlsMaterial = MtlsCertificateMaterial.Load(mtlsOptions, null, false);
        var persistenceOptions = new PersistenceOptions { DataDir = dir.Path };
        var cluster = new TopologyOptions(new ServerPeer { NodeId = nodeId, Uri = uri })
        {
            NodeId = nodeId,
            Uri = uri,
            VirtualNodes = 128,
            ReplicaCount = 1,
        };

        var offenders = new List<string>(64);
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, null));
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, static args => args.FoundationOnly = true));
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, static args => args.SecurityOptions = new SecurityOptions()));
        offenders.AddRange(await ScanAsync(cluster, mtlsMaterial, mtlsOptions, persistenceOptions, static args => args.Extensions = new ExtensionOptions()));

        Assert.Empty(offenders);
    }

    private static async Task<List<string>> ScanAsync(
        TopologyOptions cluster,
        MtlsCertificateMaterial mtls,
        MtlsOptions options,
        PersistenceOptions persistence,
        Action<ICompositionArgs>? extra)
    {
        var applicationOptions = new WebApplicationOptions
        {
            Args = [],
            ApplicationName = "Squirix.Server",
        };
        var builder = WebApplication.CreateBuilder(applicationOptions);

        // Snapshot instance-registration descriptors already present after CreateBuilder by reference identity.
        // The ASP.NET Core host itself adds several IDisposable instance services (for example its EventSource
        // logger); those are owned by the host framework and out of scope. Tracking descriptors by reference (not
        // by ServiceType) ensures that if squirrelix composition later adds a disposable instance registration with
        // the same ServiceType as a framework service, the duplicate is still flagged.
        var preExisting = new HashSet<object>(ReferenceEqualityComparer.Instance);
        foreach (var descriptor in builder.Services)
        {
            if (descriptor.ImplementationInstance is not null)
                _ = preExisting.Add(descriptor);
        }

        await ServerHostingComposition.ConfigureBuilderAsync(builder, cluster, Configure, DefaultCancellationToken);
        var offenders = new List<string>(builder.Services.Count);
        foreach (var descriptor in builder.Services)
        {
            if (!preExisting.Contains(descriptor) && descriptor.ImplementationInstance is IDisposable or IAsyncDisposable)
                offenders.Add(descriptor.ServiceType.FullName ?? descriptor.ServiceType.Name);
        }

        return offenders;

        void Configure(ICompositionArgs args)
        {
            args.MtlsMaterial = mtls;
            args.MtlsOptions = options;
            args.PersistenceOptions = persistence;
            args.WaitForRecovery = false;
            extra?.Invoke(args);
        }
    }
}
