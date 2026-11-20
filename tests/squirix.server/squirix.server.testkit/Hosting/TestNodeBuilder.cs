using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server.TestKit.Hosting;

internal static class TestNodeBuilder
{
    internal static async Task<IHostApplicationBuilder> ConfigureNodeAsync(
        this IHostApplicationBuilder builder,
        Action<SquirixServerOptions>? configure = null,
        string? settingsPath = null,
        bool loadDiscoveredSettings = true,
        Action<CompositionArgs>? configureComposition = null,
        CancellationToken cancellationToken = default)
    {
        var options = await Configurator.CreateHostingOptionsAsync(configure, settingsPath, loadDiscoveredSettings, cancellationToken).ConfigureAwait(false);
        await NodeConfiguration.ConfigureNodeAsync(builder, options, configureComposition, cancellationToken).ConfigureAwait(false);
        return builder;
    }
}
