using System;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.Node.Hosting;

namespace Squirix.Server;

/// <summary>
/// ASP.NET Core custom-hosting integration for Squirix server nodes.
/// </summary>
public static class SquirixServerAspNetCoreExtensions
{
    /// <summary>
    /// Registers a Squirix server node and configures its primary Kestrel listener.
    /// </summary>
    /// <param name="builder">The ASP.NET Core application builder.</param>
    /// <param name="configure">Optional node configuration callback applied after any loaded settings baseline.</param>
    /// <param name="settingsPath">Optional explicit settings file path.</param>
    /// <param name="loadDiscoveredSettings">When <see langword="true" />, loads a discovered <c>Squirix.settings.json</c> file before <paramref name="configure" />.</param>
    /// <param name="configureExtensions">Optional package extension configuration.</param>
    /// <returns>The supplied application builder.</returns>
    public static WebApplicationBuilder AddSquirixServer(
        this WebApplicationBuilder builder,
        Action<SquirixServerOptions>? configure = null,
        string? settingsPath = null,
        bool loadDiscoveredSettings = true,
        Action<SquirixServerExtensionOptions>? configureExtensions = null)
    {
        ArgumentNullException.ThrowIfNull(builder);

        var options = SquirixServerConfiguration.CreateHostingOptions(configure, settingsPath, loadDiscoveredSettings);
        var extensions = new SquirixServerExtensionOptions();
        configureExtensions?.Invoke(extensions);
        SquirixServerHostingComposition.ConfigureBuilder(builder, options, extensions);
        return builder;
    }

    /// <summary>
    /// Maps Squirix gRPC, health, and metrics endpoints.
    /// </summary>
    /// <param name="app">The ASP.NET Core application.</param>
    /// <returns>The supplied application.</returns>
    public static WebApplication MapSquirixServer(this WebApplication app)
    {
        ArgumentNullException.ThrowIfNull(app);
        return SquirixServerHostingComposition.MapServer(app);
    }
}
