using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using Squirix.Server.Node.Bootstrap;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Serialization;
using Squirix.Server.Utils;

namespace Squirix.Server;

/// <summary>
/// Loads and maps canonical Squirix server node configuration for hosting entry points.
/// </summary>
public static class SquirixServerConfiguration
{
    /// <summary>
    /// Applies command-line overrides used by the standalone server host.
    /// </summary>
    /// <param name="options">Server options to update.</param>
    /// <param name="devMode">When <see langword="true" />, enables local plaintext HTTP/2.</param>
    /// <param name="url">Optional URL override.</param>
    /// <param name="dataDirectory">Optional data directory override.</param>
    public static void ApplyCommandLineOverrides(SquirixServerOptions options, bool devMode, string? url, string? dataDirectory)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (devMode)
        {
            options.Url = new Uri("http://localhost:5001");
            options.AllowHttpInAnyEnvironment = true;
        }

        if (url is not null)
            options.Url = new Uri(url, UriKind.Absolute);
        if (dataDirectory is not null)
            options.DataDirectory = dataDirectory;

        ApplyRuntimeDefaults(options);
        AlignLocalPeerWithNodeUrl(options);
        ClusterTopologyValidator.Validate(options);
    }

    /// <summary>
    /// Applies runtime defaults after file or callback configuration.
    /// </summary>
    /// <param name="options">Server options to update.</param>
    public static void ApplyRuntimeDefaults(SquirixServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (options.Url.Scheme.Equals(Uri.UriSchemeHttp, StringComparison.OrdinalIgnoreCase))
            options.AllowHttpInAnyEnvironment = true;
    }

    /// <summary>
    /// Copies validated options into a target instance.
    /// </summary>
    /// <param name="source">Source options.</param>
    /// <param name="target">Target options.</param>
    public static void CopyOptions(SquirixServerOptions source, SquirixServerOptions target)
    {
        ArgumentNullException.ThrowIfNull(source);
        ArgumentNullException.ThrowIfNull(target);

        target.ClusterId = source.ClusterId;
        target.NodeId = source.NodeId;
        target.Url = source.Url;
        target.VirtualNodes = source.VirtualNodes;
        target.AllowHttpInAnyEnvironment = source.AllowHttpInAnyEnvironment;
        target.WaitForRecovery = source.WaitForRecovery;
        target.DataDirectory = source.DataDirectory;
        target.Peers.Clear();
        foreach (var peer in source.Peers)
            target.Peers.Add(new SquirixServerPeerOptions { NodeId = peer.NodeId, Url = peer.Url });
    }

    /// <summary>
    /// Creates hosting options from an optional settings file and configuration callback.
    /// </summary>
    /// <param name="configure">Optional callback applied after the settings file baseline.</param>
    /// <param name="settingsPath">Optional explicit settings path.</param>
    /// <param name="loadDiscoveredSettings">When <see langword="true" />, loads a discovered settings file before the callback.</param>
    /// <returns>Validated server options.</returns>
    public static SquirixServerOptions CreateHostingOptions(Action<SquirixServerOptions>? configure = null, string? settingsPath = null, bool loadDiscoveredSettings = true)
    {
        SquirixServerOptions options;
        if (loadDiscoveredSettings)
        {
            var path = ResolveSettingsPath(settingsPath);
            options = path is not null ? LoadFromFile(path) : new SquirixServerOptions();
        }
        else
        {
            options = new SquirixServerOptions();
        }

        configure?.Invoke(options);
        ApplyRuntimeDefaults(options);
        ClusterTopologyValidator.Validate(options);
        return options;
    }

    /// <summary>
    /// Returns <see langword="true" /> when the host portion of <paramref name="url" /> can accept a new TCP listener.
    /// </summary>
    /// <param name="url">The node URL to probe.</param>
    /// <returns><see langword="true" /> when the port appears available on loopback.</returns>
    public static bool IsListenPortAvailable(Uri url)
    {
        if (!url.IsAbsoluteUri || url.Port <= 0)
            return false;

        using var listener = new TcpListener(IPAddress.Loopback, url.Port);
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
            catch
            {
                // Best-effort release: Stop may race with listener teardown and is safe to suppress here.
            }
        }
    }

    /// <summary>
    /// Loads <c>Squirix:Cluster</c> from a settings file and validates the result.
    /// </summary>
    /// <param name="settingsFilePath">Path to the settings JSON file.</param>
    /// <returns>The validated server options.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the file is missing, invalid, or fails validation.</exception>
    public static SquirixServerOptions LoadFromFile(string settingsFilePath) =>
        !TryLoadFromFile(settingsFilePath, out var options, out var error) ? throw new InvalidOperationException(error) : options;

    /// <summary>
    /// Loads settings from the discovered settings file or creates ephemeral local defaults.
    /// </summary>
    /// <returns>Validated server options.</returns>
    public static SquirixServerOptions LoadOrCreateDefault()
    {
        var path = ResolveSettingsPath();
        if (path is not null && TryLoadFromFile(path, out var options, out _))
            return options;

        var port = NextFreePort();
        return new SquirixServerOptions
        {
            NodeId = "node",
            Url = new Uri($"https://localhost:{port}"),
        };
    }

    /// <summary>
    /// Resolves a settings file path from an explicit path or the standard discovery order.
    /// </summary>
    /// <param name="explicitPath">Optional explicit settings path.</param>
    /// <returns>The resolved path when found; otherwise <see langword="null" />.</returns>
    public static string? ResolveSettingsPath(string? explicitPath = null) => explicitPath ?? FileEx.FindFile(["Squirix.settings.json", "squirix.settings.json"]);

    /// <summary>
    /// Attempts to load <c>Squirix:Cluster</c> from a settings file.
    /// </summary>
    /// <param name="settingsFilePath">Path to the settings JSON file.</param>
    /// <param name="options">The validated options when the method succeeds.</param>
    /// <param name="error">Validation or parse error text when the method fails.</param>
    /// <returns><see langword="true" /> when loading and validation succeed.</returns>
    public static bool TryLoadFromFile(string settingsFilePath, out SquirixServerOptions options, out string? error)
    {
        options = null!;
        error = null;
        if (!File.Exists(settingsFilePath))
        {
            error = $"Settings file does not exist: {Path.GetFullPath(settingsFilePath)}";
            return false;
        }

        try
        {
            using var document = JsonDocument.Parse(
                File.ReadAllText(settingsFilePath),
                new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip });
            var root = document.RootElement;
            if (root.TryGetProperty("Squirix", out var squirix))
                root = squirix;
            if (!root.TryGetProperty("Cluster", out var cluster))
            {
                error = "Settings file must define Squirix.Cluster.";
                return false;
            }

            options = JsonSerializer.Deserialize(cluster.GetRawText(), SquirixServerHostingJsonContext.Default.SquirixServerOptions) ??
                      throw new InvalidOperationException("Cannot deserialize Squirix.Cluster.");
            if (ClusterTopologyValidator.TryValidate(options, out var failures))
                return true;
            error = string.Join(Environment.NewLine, failures);
            return false;
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            error = ex.Message;
            return false;
        }
    }

    /// <summary>
    /// Validates cluster and, when <paramref name="strict" /> is <see langword="true" />, optional settings sections.
    /// </summary>
    /// <param name="settingsFilePath">Path to the settings JSON file.</param>
    /// <param name="strict">When <see langword="true" />, also validates <c>MemoryPressure</c> and <c>PrometheusMetrics</c> sections.</param>
    /// <param name="error">Validation or parse error text when the method fails.</param>
    /// <returns><see langword="true" /> when validation succeeds.</returns>
    public static bool TryValidateSettingsFile(string settingsFilePath, bool strict, out string? error)
    {
        if (!TryLoadFromFile(settingsFilePath, out _, out error))
            return false;

        if (!strict)
            return true;

        var failures = new List<string>();
        UnifiedSettings.ValidateOptionalSections(settingsFilePath, failures);
        if (failures.Count == 0)
            return true;

        error = string.Join(Environment.NewLine, failures);
        return false;
    }

    /// <summary>
    /// Maps validated server options to internal cluster configuration.
    /// </summary>
    /// <param name="options">Validated server options.</param>
    /// <returns>Cluster configuration for the node host pipeline.</returns>
    internal static ClusterConfig ToClusterConfig(SquirixServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        ClusterTopologyValidator.Validate(options);

        var peers = new Peer[options.Peers.Count == 0 ? 1 : options.Peers.Count];
        if (options.Peers.Count == 0)
        {
            peers[0] = new Peer { NodeId = options.NodeId, Url = options.Url.AbsoluteUri };
        }
        else
        {
            for (var i = 0; i < options.Peers.Count; i++)
            {
                var peer = options.Peers[i];
                peers[i] = new Peer { NodeId = peer.NodeId, Url = peer.Url.AbsoluteUri };
            }
        }

        return new ClusterConfig
        {
            ClusterId = options.ClusterId,
            NodeId = options.NodeId,
            Url = options.Url.AbsoluteUri,
            VirtualNodes = options.VirtualNodes,
            Peers = peers,
        };
    }

    /// <summary>
    /// Aligns the local peer URL with the node URL after command-line overrides.
    /// </summary>
    /// <param name="options">Server options to update.</param>
    private static void AlignLocalPeerWithNodeUrl(SquirixServerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        for (var i = 0; i < options.Peers.Count; i++)
        {
            var peer = options.Peers[i];
            if (!string.Equals(peer.NodeId, options.NodeId, StringComparison.Ordinal))
                continue;

            if (!string.Equals(peer.Url.AbsoluteUri, options.Url.AbsoluteUri, StringComparison.OrdinalIgnoreCase))
                peer.Url = options.Url;

            return;
        }
    }

    private static int NextFreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        try
        {
            listener.Start();
            return ((IPEndPoint)listener.LocalEndpoint).Port;
        }
        finally
        {
            try
            {
                listener.Stop();
            }
            catch
            {
                // Best-effort release: Stop may race with listener teardown and is safe to suppress here.
            }
        }
    }
}
