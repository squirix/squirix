using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server;

/// <summary>ASP.NET Core custom-hosting integration for Squirix server nodes.</summary>
public static class AspNetCoreExtensions
{
    /// <summary>Registers a Squirix server node and configures its primary Kestrel listener.</summary>
    /// <param name="builder">The ASP.NET Core application builder.</param>
    /// <param name="configure">Optional node configuration callback applied after any loaded settings baseline.</param>
    /// <param name="settingsPath">Optional explicit settings file path.</param>
    /// <param name="loadDiscoveredSettings">When <see langword="true" />, loads a discovered <c language="csharp">Squirix.settings.json</c> file before <paramref name="configure" />.</param>
    /// <param name="configureExtensions">Optional package extension configuration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The supplied application builder.</returns>
    public static Task<WebApplicationBuilder> AddSquirixServerAsync(
        this WebApplicationBuilder builder,
        Action<SquirixServerOptions>? configure = null,
        string? settingsPath = null,
        bool loadDiscoveredSettings = true,
        Action<ExtensionOptions>? configureExtensions = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return ConfigureSquirixServerBuilderAsync(builder, configure, settingsPath, loadDiscoveredSettings, configureExtensions, cancellationToken);
    }

    /// <summary>Opens node storage (topology stamp, manifest, journal startup repair, replica group logs) and maps Squirix gRPC, health, and metrics endpoints.</summary>
    /// <param name="app">The built ASP.NET Core application.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The supplied application.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the data directory topology stamp refuses the configured topology, or when this application was already mapped.</exception>
    /// <exception cref="OperationCanceledException">Thrown when <paramref name="cancellationToken" /> is canceled while storage opens.</exception>
    /// <exception cref="System.IO.InvalidDataException">Thrown when persisted storage state is corrupt or unsupported.</exception>
    /// <exception cref="System.IO.IOException">Thrown when the storage files cannot be opened.</exception>
    /// <remarks>
    /// Call exactly once per application, after <c language="csharp">Build()</c> and before <c language="csharp">StartAsync</c> or
    /// <c language="csharp">RunAsync</c>. If it throws, dispose the application: the container owns every component
    /// opened so far and releases its files and locks.
    /// </remarks>
    public static Task<WebApplication> MapSquirixServerAsync(this WebApplication app, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(app);
        return ServerHostingComposition.MapServerAsync(app, cancellationToken);
    }

    private static async Task<WebApplicationBuilder> ConfigureSquirixServerBuilderAsync(
        WebApplicationBuilder builder,
        Action<SquirixServerOptions>? configure,
        string? settingsPath,
        bool loadDiscoveredSettings,
        Action<ExtensionOptions>? configureExtensions,
        CancellationToken cancellationToken)
    {
        var options = await Configurator.CreateHostingOptionsAsync(configure, settingsPath, loadDiscoveredSettings, cancellationToken).ConfigureAwait(false);
        var extensions = new ExtensionOptions();
        configureExtensions?.Invoke(extensions);
        var persistenceOptions = ResolvePersistenceOptions(options);
        await ServerHostingComposition.ConfigureBuilderAsync(
            builder,
            Configurator.ToClusterConfig(options),
            args =>
            {
                args.WaitForRecovery = options.WaitForRecovery;
                args.PersistenceOptions = persistenceOptions;
                args.Extensions = extensions;
            },
            cancellationToken).ConfigureAwait(false);
        return builder;
    }

    private static PersistenceOptions? ResolvePersistenceOptions(SquirixServerOptions options)
    {
        if (!options.PersistenceEnabled)
            return null;

        var opt = new PersistenceOptions();
        return string.IsNullOrWhiteSpace(options.DataDirectory) ? opt : opt with { DataDir = FilePathValidator.ResolveValidatedDirectoryPath(options.DataDirectory) };
    }
}
