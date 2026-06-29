using System;
using System.Globalization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Covers persistence opt-in hosting behavior.</summary>
public sealed class PersistenceHostingTests : UnitTestBase
{
    /// <summary>Ensures data directory without persistence is rejected.</summary>
    [Fact]
    public void DataDirectoryWithoutPersistenceIsRejected()
    {
        var options = new SquirixServerOptions { DataDirectory = "/tmp/data" };

        var ex = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
        Assert.Contains("UsePersistence", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Ensures the default host does not register persistence services.</summary>
    [Fact]
    public async Task DefaultHostingDoesNotRegisterPersistenceOptions()
    {
        var port = ListenPortPool.ServerUnitTests.AllocatePort();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = await builder.AddSquirixServerAsync(
            options => options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
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

        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}");
                options.UsePersistence(dir);
            },
            loadDiscoveredSettings: false,
            cancellationToken: DefaultCancellationToken);

        await using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();
        Assert.Equal(dir.Path, persistence.DataDir);
    }
}
