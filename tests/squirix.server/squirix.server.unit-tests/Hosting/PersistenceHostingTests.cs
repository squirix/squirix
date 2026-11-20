using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers persistence opt-in hosting behavior.</summary>
[Immutable]
public sealed class PersistenceHostingTests : IsolatedStorageTestBase
{
    /// <summary>Ensures the default host does not register persistence services.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultHostingSkipsPersistenceOptions(CancellationToken cancellationToken)
    {
        var services = TestNodeBuilder.CreateServices();

        _ = await services.ConfigureNodeAsync(
            static options => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        await using var app = services.BuildNode();
        _ = await Assert.That(app.Services.GetService<PersistenceOptions>()).IsNull();
    }

    /// <summary>Ensures <see cref="SquirixServerOptions.UsePersistence" /> registers persistence options.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UsePersistenceRegistersItsOptions(CancellationToken cancellationToken)
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var services = TestNodeBuilder.CreateServices();

        var optionsConfigurator = new PersistenceOptionsConfigurator(port, Dir);
        _ = await services.ConfigureNodeAsync(optionsConfigurator.Apply, loadDiscoveredSettings: false, cancellationToken: cancellationToken);

        await using var app = services.BuildNode();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();
        _ = await Assert.That(persistence.DataDir).IsEqualTo(Dir);
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
