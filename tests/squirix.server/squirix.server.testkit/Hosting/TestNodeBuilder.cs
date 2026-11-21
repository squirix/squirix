using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Logging;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit.Logging;

namespace Squirix.Server.TestKit.Hosting;

internal static class TestNodeBuilder
{
    internal static TestServiceCollection CreateServices(bool consoleLogging = true)
    {
        var services = new TestServiceCollection();
        services.AddDefaultSingleton<IServerLoggerFactory>(_ => new TestHostLoggerFactory(consoleLogging));
        TestNodeServices.AddLifetime(services);
        return services;
    }

    internal static ServerApplication BuildNode(this TestServiceCollection services) => ServerApplication.FromProvider(services.BuildServiceProvider());

    internal static async Task<TestServiceCollection> ConfigureNodeAsync(
        this TestServiceCollection services,
        Action<SquirixServerOptions>? configure = null,
        string? settingsPath = null,
        bool loadDiscoveredSettings = true,
        Action<CompositionArgs>? configureComposition = null,
        CancellationToken cancellationToken = default)
    {
        var options = await Configurator.CreateHostingOptionsAsync(configure, settingsPath, loadDiscoveredSettings, cancellationToken).ConfigureAwait(false);
        await TestNodeServices.ConfigureNodeAsync(services, options, configureComposition, cancellationToken).ConfigureAwait(false);
        return services;
    }
}
