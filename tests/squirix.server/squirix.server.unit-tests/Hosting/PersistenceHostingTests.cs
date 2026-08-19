using System;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers persistence opt-in hosting behavior.</summary>
[Immutable]
public sealed class PersistenceHostingTests : ServerUnitTestBase
{
    /// <summary>Ensures the default host does not register persistence services.</summary>
    [Fact]
    public async Task DefaultHostingDoesNotRegisterPersistenceOptions()
    {
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = await builder.AddSquirixServerAsync(
            static options => options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort())),
            loadDiscoveredSettings: false,
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        Assert.Null(app.Services.GetService<PersistenceOptions>());
    }

    /// <summary>
    /// Ensures <see cref="SquirixServerOptions.UsePersistence" /> registers persistence options.
    /// </summary>
    [Fact]
    public async Task UsePersistenceRegistersPersistenceOptions()
    {
        using var dir = new TempDirectory("squirix-persistence-tests");
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        var optionsConfigurer = new PersistenceOptionsConfigurer(port, dir.Path);
        _ = await builder.AddSquirixServerAsync(optionsConfigurer.Apply, loadDiscoveredSettings: false, cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();
        Assert.Equal(dir.Path, persistence.DataDir);
    }

    [Immutable]
    private sealed class PersistenceOptionsConfigurer
    {
        private readonly string _dataDirectory;
        private readonly int _port;

        internal PersistenceOptionsConfigurer(int port, string dataDirectory)
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
