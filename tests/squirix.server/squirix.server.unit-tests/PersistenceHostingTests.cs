using System;
using System.IO;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Http;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>
/// Covers persistence opt-in hosting behavior.
/// </summary>
public sealed class PersistenceHostingTests
{
    /// <summary>
    /// Ensures data directory without persistence is rejected.
    /// </summary>
    [Fact]
    public void DataDirectoryWithoutPersistenceIsRejected()
    {
        var options = new SquirixServerOptions { DataDirectory = "/tmp/data" };

        var ex = Assert.Throws<ArgumentException>(() => SquirixServerOptionsValidator.Validate(options));
        Assert.Contains("UsePersistence", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Ensures the default host does not register persistence services.
    /// </summary>
    [Fact]
    public void DefaultHostingDoesNotRegisterPersistenceOptions()
    {
        using var allocator = new PortAllocator(30000, 30999);
        var port = allocator.Allocate();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = builder.AddSquirixServer(options => options.Url = new Uri($"https://localhost:{port}"), loadDiscoveredSettings: false);

        using var app = builder.Build();
        Assert.Null(app.Services.GetService<PersistenceOptions>());
    }

    /// <summary>
    /// Ensures <see cref="SquirixServerOptions.UsePersistence" /> registers persistence options.
    /// </summary>
    [Fact]
    public void UsePersistenceRegistersPersistenceOptions()
    {
        var dataDir = PathKit.Combine(Path.GetTempPath(), "squirix-persistence-tests", Guid.NewGuid().ToString("N"));
        using var allocator = new PortAllocator(31000, 31999);
        var port = allocator.Allocate();
        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = "Development",
            });

        _ = builder.AddSquirixServer(
            options =>
            {
                options.Url = new Uri($"https://localhost:{port}");
                options.UsePersistence(dataDir);
            },
            loadDiscoveredSettings: false);

        using var app = builder.Build();
        var persistence = app.Services.GetRequiredService<PersistenceOptions>();
        Assert.Equal(dataDir, persistence.DataDir);

        if (Directory.Exists(dataDir))
            Directory.Delete(dataDir, true);
    }
}
