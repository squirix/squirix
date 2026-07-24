using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Utils;

namespace Squirix.Server;

/// <summary>Loads and maps canonical Squirix server node configuration for hosting entry points.</summary>
public static class Configurator
{
    private static readonly JsonDocumentOptions JsonOptions = new() { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip };

    /// <summary>Applies command-line overrides used by the standalone server host.</summary>
    /// <param name="options">Server options to update.</param>
    /// <param name="uri">Optional URL override.</param>
    /// <param name="dataDirectory">Optional data directory override.</param>
    /// <param name="persist">When <see langword="true" />, enables journal/snapshot persistence.</param>
    public static void ApplyCommandLineOverrides(SquirixServerOptions options, Uri? uri, string? dataDirectory, bool persist = false)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (uri is not null)
            options.Uri = uri;
        if (persist)
            options.UsePersistence();
        if (dataDirectory is not null)
            options.DataDirectory = FilePathValidator.ResolveValidatedDirectoryPath(dataDirectory);

        ApplyRuntimeDefaults(options);
        AlignLocalPeerWithNodeUrl(options);
        SquirixServerOptionsValidator.Validate(options);
    }

    /// <summary>Applies runtime defaults after file or callback configuration.</summary>
    /// <param name="options">Server options to update.</param>
    public static void ApplyRuntimeDefaults(SquirixServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.DataDirectory is not null)
            options.DataDirectory = FilePathValidator.ResolveValidatedDirectoryPath(options.DataDirectory);
    }

    /// <summary>Copies validated options into a target instance.</summary>
    /// <param name="source">Source options.</param>
    /// <param name="target">Target options.</param>
    public static void CopyOptions(SquirixServerOptions source, SquirixServerOptions target)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);

        target.ClusterId = source.ClusterId;
        target.NodeId = source.NodeId;
        target.Uri = source.Uri;
        target.VirtualNodes = source.VirtualNodes;
        target.WaitForRecovery = source.WaitForRecovery;
        target.PersistenceEnabled = source.PersistenceEnabled;
        target.DataDirectory = source.DataDirectory;
        var peers = new SquirixServerPeerOptions[source.Peers.Count];
        for (var i = 0; i < peers.Length; i++)
        {
            var peer = source.Peers[i];
            peers[i] = new SquirixServerPeerOptions { NodeId = peer.NodeId, Uri = peer.Uri };
        }

        target.Peers = peers;
    }

    /// <summary>Creates hosting options from an optional settings file and configuration callback.</summary>
    /// <param name="configure">Optional callback applied after the settings file baseline.</param>
    /// <param name="settingsPath">Optional explicit settings path.</param>
    /// <param name="loadDiscoveredSettings">When <see langword="true" />, loads a discovered settings file before the callback.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Validated server options.</returns>
    public static async Task<SquirixServerOptions> CreateHostingOptionsAsync(
        Action<SquirixServerOptions>? configure = null,
        string? settingsPath = null,
        bool loadDiscoveredSettings = true,
        CancellationToken cancellationToken = default)
    {
        SquirixServerOptions options;
        if (loadDiscoveredSettings)
        {
            var path = ResolveSettingsPath(settingsPath);
            options = path is not null ? await LoadFromFileAsync(path, cancellationToken).ConfigureAwait(false) : new SquirixServerOptions();
        }
        else
        {
            options = new SquirixServerOptions();
        }

        configure?.Invoke(options);
        ApplyRuntimeDefaults(options);
        SquirixServerOptionsValidator.Validate(options);
        return options;
    }

    /// <summary>
    /// Returns <see langword="true" /> when the host portion of <paramref name="uri" /> can accept a new TCP listener.
    /// </summary>
    /// <param name="uri">The node URL to probe.</param>
    /// <returns><see langword="true" /> when the port appears available on loopback.</returns>
    public static bool IsListenPortAvailable(Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (!uri.IsAbsoluteUri || uri.Port <= 0)
            return false;

        using var listener = new TcpListener(IPAddress.Loopback, uri.Port);
        try
        {
            listener.Start();
            return true;
        }
        catch (SocketException)
        {
            return false;
        }
        finally
        {
            try
            {
                listener.Stop();
            }
            catch (ObjectDisposedException)
            {
                // Best-effort release: Stop may race with listener teardown and is safe to suppress here.
            }
            catch (SocketException)
            {
                // Best-effort release: Stop may race with listener teardown and is safe to suppress here.
            }
        }
    }

    /// <summary>
    /// Loads <c>Squirix:Cluster</c> from a settings file and validates the result.
    /// </summary>
    /// <param name="settingsFilePath">Path to the settings JSON file.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The validated server options.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the file is missing, invalid, or fails validation.</exception>
    public static async Task<SquirixServerOptions> LoadFromFileAsync(string settingsFilePath, CancellationToken cancellationToken = default)
    {
        var (success, options, error) = await TryLoadFromFileAsync(settingsFilePath, cancellationToken).ConfigureAwait(false);
        if (!success)
            throw new InvalidOperationException(error);

        return options ?? throw new InvalidOperationException("Settings file did not produce cluster options.");
    }

    /// <summary>Loads settings from the discovered settings file or creates ephemeral local defaults.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Validated server options.</returns>
    public static async Task<SquirixServerOptions> LoadOrCreateDefaultAsync(CancellationToken cancellationToken = default)
    {
        var path = ResolveSettingsPath();
        if (path is not null)
        {
            var (success, options, _) = await TryLoadFromFileAsync(path, cancellationToken).ConfigureAwait(false);
            if (success && options is not null)
                return options;
        }

        var port = NextFreePort();
        return new SquirixServerOptions
        {
            NodeId = "node",
            Uri = new Uri($"https://localhost:{port.ToString(CultureInfo.InvariantCulture)}"),
        };
    }

    /// <summary>Validates and canonicalizes an operator-supplied data directory path.</summary>
    /// <param name="dataDirectory">Absolute or relative data directory path.</param>
    /// <returns>Normalized absolute directory path.</returns>
    /// <exception cref="ArgumentException">Thrown when the path is empty, contains invalid characters, or has <c>.</c> / <c>..</c> segments.</exception>
    public static string ResolveValidatedDataDirectory(string dataDirectory) => FilePathValidator.ResolveValidatedDirectoryPath(dataDirectory);

    /// <summary>Validates and canonicalizes an operator-supplied file path.</summary>
    /// <param name="path">Absolute or relative file path.</param>
    /// <returns>Normalized absolute file path.</returns>
    /// <exception cref="ArgumentException">Thrown when the path is empty, contains invalid characters, or has <c>.</c> / <c>..</c> segments.</exception>
    public static string ResolveValidatedFilePath(string path) => FilePathValidator.ResolveValidatedFilePath(path);

    /// <summary>Resolves a settings file path from an explicit path or the standard discovery order.</summary>
    /// <param name="explicitPath">Optional explicit settings path.</param>
    /// <returns>The resolved path when found; otherwise <see langword="null" />.</returns>
    public static string? ResolveSettingsPath(string? explicitPath = null) => explicitPath is null ? FileEx.FindFile(["Squirix.settings.json", "squirix.settings.json"])
        : ResolveValidatedFilePath(explicitPath);

    /// <summary>
    /// Attempts to load <c>Squirix:Cluster</c> from a settings file.
    /// </summary>
    /// <param name="settingsFilePath">Path to the settings JSON file.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Success</c> is <see langword="true" /> when loading and validation succeed,
    /// <c>Options</c> holds the validated options, and <c>Error</c> holds failure text when applicable.
    /// </returns>
    public static async Task<(bool Success, SquirixServerOptions? Options, string? Error)> TryLoadFromFileAsync(
        string settingsFilePath,
        CancellationToken cancellationToken = default)
    {
        string validatedPath;
        try
        {
            validatedPath = FilePathValidator.ResolveValidatedFilePath(settingsFilePath);
        }
        catch (ArgumentException ex)
        {
            return (false, null, ex.Message);
        }

        if (!File.Exists(validatedPath))
            return (false, null, $"Settings file does not exist: {validatedPath}");

        try
        {
            var bytes = await File.ReadAllBytesAsync(validatedPath, cancellationToken).ConfigureAwait(false);
            using var document = JsonDocument.Parse(bytes, JsonOptions);
            var root = document.RootElement;
            if (root.TryGetProperty("Squirix", out var squirix))
                root = squirix;
            if (!root.TryGetProperty("Cluster", out var cluster))
                return (false, null, "Settings file must define Squirix.Cluster.");

            var options = JsonSerializer.Deserialize(cluster.GetRawText(), SquirixServerHostingJsonContext.Default.SquirixServerOptions) ??
                          throw new InvalidOperationException("Cannot deserialize Squirix.Cluster.");
            if (options.DataDirectory is not null)
                options.DataDirectory = FilePathValidator.ResolveValidatedDirectoryPath(options.DataDirectory);

            if (SquirixServerOptionsValidator.TryValidate(options, out var failures))
                return (true, options, null);

            return (false, null, string.Join(Environment.NewLine, failures));
        }
        catch (ArgumentException ex)
        {
            return (false, null, ex.Message);
        }
        catch (JsonException ex)
        {
            return (false, null, ex.Message);
        }
        catch (InvalidOperationException ex)
        {
            return (false, null, ex.Message);
        }
    }

    /// <summary>
    /// Validates cluster and, when <paramref name="strict" /> is <see langword="true" />, optional settings sections.
    /// </summary>
    /// <param name="settingsFilePath">Path to the settings JSON file.</param>
    /// <param name="strict">When <see langword="true" />, also validates <c>MemoryPressure</c>, <c>Snapshot</c>, and <c>PrometheusMetrics</c> sections.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>
    /// A tuple where <c>Success</c> is <see langword="true" /> when validation succeeds and <c>Error</c> holds failure text when applicable.
    /// </returns>
    public static async Task<(bool Success, string? Error)> TryValidateSettingsFileAsync(string settingsFilePath, bool strict, CancellationToken cancellationToken = default)
    {
        var (success, _, error) = await TryLoadFromFileAsync(settingsFilePath, cancellationToken).ConfigureAwait(false);
        if (!success)
            return (false, error);

        if (!strict)
            return (true, null);

        var failures = new List<string>();
        await UnifiedSettings.ValidateOptionalSectionsAsync(settingsFilePath, failures, cancellationToken).ConfigureAwait(false);

        return failures.Count is 0 ? (true, null) : (false, string.Join(Environment.NewLine, failures));
    }

    /// <summary>Maps validated server options to internal cluster configuration.</summary>
    /// <param name="options">Validated server options.</param>
    /// <returns>Cluster configuration for the node host pipeline.</returns>
    internal static TopologyOptions ToClusterConfig(SquirixServerOptions options)
    {
        SquirixServerOptionsValidator.Validate(options);

        var peers = new ServerPeer[options.Peers.Count is 0 ? 1 : options.Peers.Count];
        if (options.Peers.Count is 0)
        {
            peers[0] = new ServerPeer { NodeId = options.NodeId, Uri = options.Uri };
        }
        else
            for (var i = 0; i < options.Peers.Count; i++)
            {
                var peer = options.Peers[i];
                peers[i] = new ServerPeer { NodeId = peer.NodeId, Uri = peer.Uri };
            }

        return new TopologyOptions(peers)
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            Uri = options.Uri,
            VirtualNodes = options.VirtualNodes,
        };
    }

    /// <summary>Aligns the local peer URL with the node URL after command-line overrides.</summary>
    /// <param name="options">Server options to update.</param>
    private static void AlignLocalPeerWithNodeUrl(SquirixServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        for (var i = 0; i < options.Peers.Count; i++)
        {
            var peer = options.Peers[i];
            if (!string.Equals(peer.NodeId, options.NodeId, StringComparison.Ordinal))
                continue;

            if (!string.Equals(peer.Uri.AbsoluteUri, options.Uri.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
                peer.Uri = options.Uri;

            return;
        }
    }

    private static int NextFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        try
        {
            listener.Start();
            if (listener.LocalEndpoint is not IPEndPoint endpoint)
                throw new InvalidOperationException("TcpListener did not expose a local IPEndPoint.");

            return endpoint.Port;
        }
        finally
        {
            try
            {
                listener.Stop();
            }
            catch (ObjectDisposedException)
            {
                // Best-effort release: Stop may race with listener teardown and is safe to suppress here.
            }
            catch (SocketException)
            {
                // Best-effort release: Stop may race with listener teardown and is safe to suppress here.
            }
        }
    }
}
