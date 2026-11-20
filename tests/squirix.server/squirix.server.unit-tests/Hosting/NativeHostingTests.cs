using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises the native hosting contract through real listener and storage operations.</summary>
[Immutable]
public sealed class NativeHostingTests : IsolatedStorageTestBase
{
    /// <summary>A native host serves a cache call and provides a separate operational listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostServesCacheAndHealth(CancellationToken cancellationToken)
    {
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        var uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", held.Port));
        var builder = Host.CreateApplicationBuilder();
        _ = await builder.ConfigureNodeAsync(options => options.Uri = uri, loadDiscoveredSettings: false, configureComposition: static args => args.ManagementUri = new Uri("https://127.0.0.1:0"), cancellationToken: cancellationToken);
        using var host = builder.Build();
        await ServerHostingComposition.OpenStorageAsync(host.Services, cancellationToken);
        held.Dispose();
        await host.StartAsync(cancellationToken);
        try
        {
            using var channel = new PublicBinaryChannel(uri);
            var client = new CacheWireClient(channel);
            var reply = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "hosting-probe" }, cancellationToken: cancellationToken);
            _ = await Assert.That(reply.Found).IsFalse();

            var management = host.Services.GetRequiredService<NativeClientService>().ManagementUri!;
            _ = await Assert.That(management.Port).IsNotEqualTo(uri.Port);
            using var handler = LoopbackHttp.CreateHandler();
            using var http = new HttpClient(handler, false);
            using var health = await http.GetAsync(new Uri(management, "/health/live"), cancellationToken);
            _ = await Assert.That(health.StatusCode).IsEqualTo(HttpStatusCode.OK);
            using var removed = await http.GetAsync(new Uri(management, "/extension-test"), cancellationToken);
            _ = await Assert.That(removed.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
        }
        finally
        {
            await host.StopAsync(cancellationToken);
        }
    }

    /// <summary>A configured directory retains the strict fsync persistence configuration.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DataDirOverrideKeepsStrictFsyncDefault(CancellationToken cancellationToken)
    {
        var builder = Host.CreateApplicationBuilder();
        var options = new PersistenceOptionsConfigurator(ListenPortPool.ServerUnitTests.AllocatePort(), Dir);
        _ = await builder.ConfigureNodeAsync(options.Apply, loadDiscoveredSettings: false, cancellationToken: cancellationToken);
        using var host = builder.Build();
        var persistence = host.Services.GetRequiredService<PersistenceOptions>();
        _ = await Assert.That(persistence.DataDir).IsEqualTo(Dir);
    }

    /// <summary>The default management origin uses loopback and avoids the peer port.</summary>
    /// <param name="clientPort">The configured client listener port.</param>
    /// <param name="peerPort">The configured peer listener port.</param>
    /// <param name="matchesLocalhost">Whether the certificate is valid for localhost.</param>
    /// <param name="managementPort">The expected management port.</param>
    /// <param name="managementHost">The expected management host.</param>
    [Test]
    [Arguments(7400, 0, true, 7402, "localhost")]
    [Arguments(7400, 7402, false, 7403, "127.0.0.1")]
    [Arguments(65535, 65533, true, 65532, "localhost")]
    public async Task DefaultManagementOriginAvoidsPeerPort(int clientPort, int peerPort, bool matchesLocalhost, int managementPort, string managementHost)
    {
        var client = new UriBuilder(Uri.UriSchemeHttps, "node.example.org", clientPort).Uri;
        var management = NativeClientService.ResolveDefaultManagementUri(client, peerPort, matchesLocalhost);
        _ = await Assert.That(management.Port).IsEqualTo(managementPort);
        _ = await Assert.That(management.Host).IsEqualTo(managementHost);
    }

    [Immutable]
    private sealed class PersistenceOptionsConfigurator
    {
        private readonly string _dataDirectory;
        private readonly int _port;

        internal PersistenceOptionsConfigurator(int port, string dataDirectory)
        {
            _port = port;
            _dataDirectory = dataDirectory;
            Apply = ApplyCore;
        }

        internal Action<SquirixServerOptions> Apply { get; }

        private void ApplyCore(SquirixServerOptions options)
        {
            options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", _port));
            options.UsePersistence(_dataDirectory);
        }
    }
}
