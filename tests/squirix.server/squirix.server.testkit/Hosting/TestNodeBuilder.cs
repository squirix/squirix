using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.Hosting;

internal static class TestNodeBuilder
{
    internal static IServiceCollection CreateServices(Action<ILoggingBuilder>? configureLogging = null)
    {
        var services = new ServiceCollection();
        _ = services.AddLogging(configureLogging ?? TestHostLogging.Configure);
        TestNodeServices.AddLifetime(services);
        return services;
    }

    internal static ServerApplication BuildNode(this IServiceCollection services) => ServerApplication.FromProvider(services.BuildServiceProvider());

    internal static async Task<IServiceCollection> ConfigureNodeAsync(
        this IServiceCollection services,
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
