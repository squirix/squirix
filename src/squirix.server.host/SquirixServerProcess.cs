using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;

namespace Squirix.Server.Host;

internal static class SquirixServerProcess
{
    internal static async Task<int> RunAsync(string[] args)
    {
        try
        {
            var command = SquirixServerCommand.Parse(args);
            return command.Name switch
            {
                "run" => await RunServerAsync(command).ConfigureAwait(false),
                "init" => Initialize(command),
                "validate-config" => ValidateConfig(command),
                "doctor" => Doctor(command),
                "version" => Version(),
                "help" => Help(),
                _ => throw new InvalidOperationException($"Unknown command '{command.Name}'. Run 'squirix-server help'."),
            };
        }
        catch (InvalidOperationException ex)
        {
            await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}");
            return 1;
        }
        catch (IOException ex)
        {
            await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}");
            return 1;
        }
        catch (UnauthorizedAccessException ex)
        {
            await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}");
            return 1;
        }
        catch (ArgumentException ex)
        {
            await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}");
            return 1;
        }
    }

    private static int Doctor(SquirixServerCommand command)
    {
        var options = LoadOptions(command);
        Console.WriteLine("[Squirix.Server] Doctor");
        Console.WriteLine($"  Runtime: {Environment.Version}");
        Console.WriteLine($"  OS: {Environment.OSVersion}");
        Console.WriteLine($"  Cluster ID: {options.ClusterId}");
        Console.WriteLine($"  Node ID: {options.NodeId}");
        Console.WriteLine($"  URL: {options.Url}");
        Console.WriteLine($"  Peers: {(options.Peers.Count == 0 ? 1 : options.Peers.Count)} configured");
        Console.WriteLine(SquirixServerConfiguration.IsListenPortAvailable(options.Url) ? "  Listen port: available" : "  Listen port: NOT available (already in use)");
        WritePersistenceStatus(options);
        Console.WriteLine("  Configuration: valid");
        return 0;
    }

    private static int Help()
    {
        Console.WriteLine(
            """
            Squirix.Server.Host

            Commands:
              run [--strict] [--persist] [--urls URL] [--data-dir PATH] [--settings PATH]
              init [--settings PATH]
              validate-config --settings PATH [--strict]
              doctor [--strict] [--persist] [--urls URL] [--data-dir PATH] [--settings PATH]
              version
              help
            """);
        return 0;
    }

    private static int Initialize(SquirixServerCommand command)
    {
        var path = command.SettingsPath ?? "Squirix.settings.json";
        if (File.Exists(path))
            throw new InvalidOperationException($"Settings file already exists: {Path.GetFullPath(path)}");

        File.Copy(Path.Join(AppContext.BaseDirectory, "Squirix.settings.default.json"), path);
        _ = SquirixServerSettings.Load(path);
        Console.WriteLine($"[Squirix.Server] Created settings: {Path.GetFullPath(path)}");
        return 0;
    }

    private static SquirixServerOptions LoadOptions(SquirixServerCommand command)
    {
        var settingsPath = ResolveSettingsPath(command);
        var options = settingsPath is null ? new SquirixServerOptions() : SquirixServerSettings.Load(settingsPath);
        SquirixServerConfiguration.ApplyCommandLineOverrides(options, command.Url, command.DataDirectory, command.Persist);
        return options;
    }

    private static string? ResolveSettingsPath(SquirixServerCommand command) => SquirixServerConfiguration.ResolveSettingsPath(command.SettingsPath);

    private static async Task<int> RunServerAsync(SquirixServerCommand command)
    {
        var options = LoadOptions(command);
        var builder = WebApplication.CreateBuilder();
        _ = builder.AddSquirixServer(target => SquirixServerConfiguration.CopyOptions(options, target), null, false);
        await using var app = builder.Build();
        _ = app.MapSquirixServer();

        await app.StartAsync().ConfigureAwait(false);
        Console.WriteLine("[Squirix.Server] Server is ready.");
        Console.WriteLine($"  gRPC endpoint: {options.Url}");
        Console.WriteLine($"  Health endpoint: {options.Url}/health");
        Console.WriteLine($"  Metrics endpoint: {options.Url}/metrics");
        Console.WriteLine($"  Node ID: {options.NodeId}");
        WritePersistenceStatus(options);
        Console.WriteLine($"  Settings: {ResolveSettingsPath(command) ?? "<defaults>"}");
        Console.WriteLine();
        Console.WriteLine("Client:");
        Console.WriteLine($"await using var client = await SquirixClient.ConnectAsync(\"{options.Url}\");");
        Console.WriteLine();
        Console.WriteLine("Waiting for shutdown (Ctrl+C)...");

        using var shutdown = new ShutdownSignal();
        await app.WaitForShutdownAsync(shutdown.Token).ConfigureAwait(false);
        return 0;
    }

    private static int ValidateConfig(SquirixServerCommand command)
    {
        if (command.SettingsPath is null)
            throw new InvalidOperationException("validate-config requires --settings PATH.");

        if (!SquirixServerConfiguration.TryValidateSettingsFile(command.SettingsPath, command.Strict, out var error))
            throw new InvalidOperationException(error);

        var scope = command.Strict ? "full settings" : "cluster settings";
        Console.WriteLine($"[Squirix.Server] {scope} valid: {Path.GetFullPath(command.SettingsPath)}");
        return 0;
    }

    private static int Version()
    {
        var version = typeof(SquirixServerProcess).Assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>()?.InformationalVersion ??
                      typeof(SquirixServerProcess).Assembly.GetName().Version?.ToString() ?? "unknown";
        Console.WriteLine(version);
        return 0;
    }

    private static void WritePersistenceStatus(SquirixServerOptions options)
    {
        if (!options.PersistenceEnabled)
        {
            Console.WriteLine("  Persistence: disabled");
            return;
        }

        var dataDirectory = options.DataDirectory ?? "<default>";
        Console.WriteLine($"  Persistence: enabled (data dir: {dataDirectory})");
        if (string.IsNullOrWhiteSpace(options.DataDirectory))
            return;

        var dataDirectoryPath = options.DataDirectory;
        try
        {
            _ = Directory.CreateDirectory(dataDirectoryPath);
            var probe = Path.Join(dataDirectoryPath, ".squirix-doctor-probe");
            File.WriteAllText(probe, string.Empty);
            File.Delete(probe);
            Console.WriteLine("  Data directory access: writable");
        }
        catch (IOException ex)
        {
            Console.WriteLine($"  Data directory access: NOT writable ({ex.Message})");
        }
        catch (UnauthorizedAccessException ex)
        {
            Console.WriteLine($"  Data directory access: NOT writable ({ex.Message})");
        }
    }
}
