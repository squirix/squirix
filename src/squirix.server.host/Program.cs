using System;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;

namespace Squirix.Server.Host;

[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Entry point type is already internal; analyzer still reports this file.")]
internal static class Program
{
    private static Task<int> Main(string[] args) => SquirixServerProcess.RunAsync(args);

    private static class SquirixServerProcess
    {
        private const string HelpText = "Squirix.Server.Host\n\n" + "Commands:\n" + "  run [--strict] [--persist] [--urls URL] [--data-dir PATH] [--settings PATH]\n" +
                                        "  init [--settings PATH]\n" + "  validate-config --settings PATH [--strict]\n" +
                                        "  doctor [--strict] [--persist] [--urls URL] [--data-dir PATH] [--settings PATH]\n" + "  version\n" + "  help\n";

        internal static async Task<int> RunAsync(string[] args)
        {
            try
            {
                var command = SquirixServerCommand.Parse(args);
                return command.Name switch
                {
                    "run" => await RunServerAsync(command).ConfigureAwait(false),
                    "init" => await InitializeAsync(command).ConfigureAwait(false),
                    "validate-config" => await ValidateConfigAsync(command).ConfigureAwait(false),
                    "doctor" => await DoctorAsync(command).ConfigureAwait(false),
                    "version" => Version(),
                    "help" => Help(),
                    _ => throw new InvalidOperationException($"Unknown command '{command.Name}'. Run 'squirix-server help'."),
                };
            }
            catch (InvalidOperationException ex)
            {
                await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}").ConfigureAwait(false);
                return 1;
            }
            catch (IOException ex)
            {
                await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}").ConfigureAwait(false);
                return 1;
            }
            catch (UnauthorizedAccessException ex)
            {
                await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}").ConfigureAwait(false);
                return 1;
            }
            catch (ArgumentException ex)
            {
                await Console.Error.WriteLineAsync($"[Squirix.Server] Error: {ex.Message}").ConfigureAwait(false);
                return 1;
            }
        }

        private static async Task<int> DoctorAsync(SquirixServerCommand command)
        {
            var options = await LoadOptionsAsync(command, CancellationToken.None).ConfigureAwait(false);
            await Console.Out.WriteLineAsync("[Squirix.Server] Doctor").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Runtime: {Environment.Version}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  OS: {Environment.OSVersion}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Cluster ID: {options.ClusterId}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Node ID: {options.NodeId}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  URL: {options.Uri}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Peers: {(options.Peers.Count is 0 ? 1 : options.Peers.Count).ToString(CultureInfo.InvariantCulture)} configured")
                         .ConfigureAwait(false);
            await Console.Out.WriteLineAsync(
                Configurator.IsListenPortAvailable(options.Uri) ? "  Listen port: available" : "  Listen port: NOT available (already in use)").ConfigureAwait(false);
            await WritePersistenceStatusAsync(options, CancellationToken.None).ConfigureAwait(false);
            await Console.Out.WriteLineAsync("  Configuration: valid").ConfigureAwait(false);
            return 0;
        }

        private static int Help()
        {
            Console.Out.WriteLine(HelpText);
            return 0;
        }

        private static async Task<int> InitializeAsync(SquirixServerCommand command)
        {
            var path = command.SettingsPath ?? "Squirix.settings.json";
            if (File.Exists(path))
                throw new InvalidOperationException($"Settings file already exists: {Path.GetFullPath(path)}");

            File.Copy(Path.Join(AppContext.BaseDirectory, "Squirix.settings.default.json"), path);
            _ = await LoadSettingsAsync(path, CancellationToken.None).ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"[Squirix.Server] Created settings: {Path.GetFullPath(path)}").ConfigureAwait(false);
            return 0;
        }

        private static async Task<SquirixServerOptions> LoadOptionsAsync(SquirixServerCommand command, CancellationToken cancellationToken = default)
        {
            var settingsPath = ResolveSettingsPath(command);
            var options = settingsPath is null ? new SquirixServerOptions() : await LoadSettingsAsync(settingsPath, cancellationToken).ConfigureAwait(false);
            Configurator.ApplyCommandLineOverrides(options, command.Uri, command.DataDirectory, command.Persist);
            return options;
        }

        private static Task<SquirixServerOptions> LoadSettingsAsync(string path, CancellationToken cancellationToken = default) =>
            Configurator.LoadFromFileAsync(path, cancellationToken);

        private static string? ResolveSettingsPath(SquirixServerCommand command) => Configurator.ResolveSettingsPath(command.SettingsPath);

        private static async Task<int> RunServerAsync(SquirixServerCommand command)
        {
            var options = await LoadOptionsAsync(command, CancellationToken.None).ConfigureAwait(false);
            var builder = WebApplication.CreateBuilder();
            _ = await builder.AddSquirixServerAsync(
                target => Configurator.CopyOptions(options, target),
                loadDiscoveredSettings: false,
                cancellationToken: CancellationToken.None).ConfigureAwait(false);
            var app = builder.Build();
            await using (app.ConfigureAwait(false))
            {
                _ = app.MapSquirixServer();
                await app.StartAsync(app.Lifetime.ApplicationStopping).ConfigureAwait(false);
                await WriteRunServerStatusAsync(command, options, CancellationToken.None).ConfigureAwait(false);
                await app.WaitForShutdownAsync(app.Lifetime.ApplicationStopping).ConfigureAwait(false);
                return 0;
            }
        }

        private static async Task<int> ValidateConfigAsync(SquirixServerCommand command)
        {
            if (command.SettingsPath is null)
                throw new InvalidOperationException("validate-config requires --settings PATH.");

            var (success, error) = await Configurator.TryValidateSettingsFileAsync(command.SettingsPath, command.Strict, CancellationToken.None)
                                                                   .ConfigureAwait(false);
            if (!success)
                throw new InvalidOperationException(error);

            var scope = command.Strict ? "full settings" : "cluster settings";
            await Console.Out.WriteLineAsync($"[Squirix.Server] {scope} valid: {Path.GetFullPath(command.SettingsPath)}").ConfigureAwait(false);
            return 0;
        }

        private static int Version()
        {
            Console.Out.WriteLine(BuildMetadata.InformationalVersion);
            return 0;
        }

        private static async Task WritePersistenceStatusAsync(SquirixServerOptions options, CancellationToken cancellationToken)
        {
            if (!options.PersistenceEnabled)
            {
                await Console.Out.WriteLineAsync("  Persistence: disabled").ConfigureAwait(false);
                return;
            }

            var dataDirectory = options.DataDirectory ?? "<default>";
            await Console.Out.WriteLineAsync($"  Persistence: enabled (data dir: {dataDirectory})").ConfigureAwait(false);
            if (string.IsNullOrWhiteSpace(options.DataDirectory))
                return;

            var dataDirectoryPath = options.DataDirectory;
            try
            {
                _ = Directory.CreateDirectory(dataDirectoryPath);
                var probe = Path.Join(dataDirectoryPath, ".squirix-doctor-probe");
                await File.WriteAllTextAsync(probe, string.Empty, cancellationToken).ConfigureAwait(false);
                File.Delete(probe);
                await Console.Out.WriteLineAsync("  Data directory access: writable").ConfigureAwait(false);
            }
            catch (IOException ex)
            {
                await Console.Out.WriteLineAsync($"  Data directory access: NOT writable ({ex.Message})").ConfigureAwait(false);
            }
            catch (UnauthorizedAccessException ex)
            {
                await Console.Out.WriteLineAsync($"  Data directory access: NOT writable ({ex.Message})").ConfigureAwait(false);
            }
        }

        private static async Task WriteRunServerStatusAsync(SquirixServerCommand command, SquirixServerOptions options, CancellationToken cancellationToken)
        {
            await Console.Out.WriteLineAsync("[Squirix.Server] Server is ready.").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  URL: {options.Uri}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Health endpoint: {options.Uri}/health").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Metrics endpoint: {options.Uri}/metrics").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Node ID: {options.NodeId}").ConfigureAwait(false);
            await WritePersistenceStatusAsync(options, cancellationToken).ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"  Settings: {ResolveSettingsPath(command) ?? "<defaults>"}").ConfigureAwait(false);
            await Console.Out.WriteLineAsync().ConfigureAwait(false);
            await Console.Out.WriteLineAsync("Client:").ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"await using var client = await SquirixClient.ConnectAsync(new Uri(\"{options.Uri}\"));").ConfigureAwait(false);
            await Console.Out.WriteLineAsync().ConfigureAwait(false);
            await Console.Out.WriteLineAsync("Waiting for shutdown (Ctrl+C)...").ConfigureAwait(false);
        }

        private sealed record SquirixServerCommand(string Name, bool Strict, Uri? Uri, string? DataDirectory, bool Persist, string? SettingsPath)
        {
            internal static SquirixServerCommand Parse(string[] args)
            {
                var name = args.Length is 0 || args[0].StartsWith("--", StringComparison.Ordinal) ? "run" : args[0];
                var start = string.Equals(name, "run", StringComparison.OrdinalIgnoreCase) && (args.Length is 0 || args[0].StartsWith("--", StringComparison.Ordinal)) ? 0 : 1;
                var strict = false;
                var persist = false;
                Uri? uri = null;
                string? dataDir = null;
                string? settings = null;

                for (var i = start; i < args.Length; i++)
                {
                    switch (args[i])
                    {
                        case "--strict":
                            strict = true;
                            break;
                        case "--persist":
                            persist = true;
                            break;
                        case "--urls":
                            uri = new Uri(ReadValue(args, ref i), UriKind.Absolute);
                            break;
                        case "--data-dir":
                            dataDir = ReadValue(args, ref i);
                            break;
                        case "--settings":
                            settings = ReadValue(args, ref i);
                            break;
                        case "--help":
                        case "-h":
                            return new SquirixServerCommand("help", false, null, null, false, null);
                        default:
                            throw new InvalidOperationException($"Unknown argument '{args[i]}'.");
                    }
                }

                return new SquirixServerCommand(name, strict, uri, dataDir, persist, settings);
            }

            private static string ReadValue(string[] args, ref int index)
            {
                index++;
                if (index >= args.Length || args[index].StartsWith("--", StringComparison.Ordinal))
                    throw new InvalidOperationException($"Argument '{args[index - 1]}' requires a value.");
                return args[index];
            }
        }
    }
}
