using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.Hosting;
using Squirix.Server.Attributes;

namespace Squirix.Server.Host;

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
            await Console.Out.WriteLineAsync($"  Peers: {(options.Peers.Count == 0 ? 1 : options.Peers.Count).ToString(CultureInfo.InvariantCulture)} configured")
                         .ConfigureAwait(false);
            await Console.Out.WriteLineAsync(Configurator.IsListenPortAvailable(options.Uri) ? "  Listen port: available" : "  Listen port: NOT available (already in use)")
                         .ConfigureAwait(false);
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
            var path = Configurator.ResolveSettingsPath(command.SettingsPath ?? "Squirix.settings.json")!;
            if (File.Exists(path))
                throw new InvalidOperationException($"Settings file already exists: {path}");

            File.Copy(Path.Join(AppContext.BaseDirectory, "Squirix.settings.default.json"), path);
            _ = await LoadSettingsAsync(path, CancellationToken.None).ConfigureAwait(false);
            await Console.Out.WriteLineAsync($"[Squirix.Server] Created settings: {path}").ConfigureAwait(false);
            return 0;
        }

        private static async Task<SquirixServerOptions> LoadOptionsAsync(SquirixServerCommand command, CancellationToken cancellationToken = default)
        {
            var settingsPath = ResolveSettingsPath(command);
            var options = settingsPath == null ? new SquirixServerOptions() : await LoadSettingsAsync(settingsPath, cancellationToken).ConfigureAwait(false);
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
            _ = await builder.AddSquirixServerAsync(target => Configurator.CopyOptions(options, target), loadDiscoveredSettings: false, cancellationToken: CancellationToken.None)
                             .ConfigureAwait(false);
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
            if (command.SettingsPath == null)
                throw new InvalidOperationException("validate-config requires --settings PATH.");

            var (success, error) = await Configurator.TryValidateSettingsFileAsync(command.SettingsPath, command.Strict, CancellationToken.None).ConfigureAwait(false);
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

            var dataDirectoryPath = Configurator.ResolveValidatedDataDirectory(options.DataDirectory);
            try
            {
                _ = Directory.CreateDirectory(dataDirectoryPath);
                var probe = Configurator.ResolveValidatedFilePath(Path.Join(dataDirectoryPath, ".squirix-doctor-probe"));
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

        /// <summary>Parsed CLI command and option flags for the standalone server host.</summary>
        /// <param name="Name">Command name such as <c language="csharp">run</c>, <c language="csharp">init</c>, or <c language="csharp">help</c>.</param>
        /// <param name="Strict">Whether strict configuration validation is requested.</param>
        /// <param name="Uri">Optional listen URI override from <c language="csharp">--urls</c>.</param>
        /// <param name="DataDirectory">Optional data directory override from <c language="csharp">--data-dir</c>.</param>
        /// <param name="Persist">Whether persistence was requested via <c language="csharp">--persist</c>.</param>
        /// <param name="SettingsPath">Optional settings file path from <c language="csharp">--settings</c>.</param>
        [Immutable]
        private sealed record SquirixServerCommand(string Name, bool Strict, Uri? Uri, string? DataDirectory, bool Persist, string? SettingsPath)
        {
            internal static SquirixServerCommand Parse(string[] args)
            {
                var name = ResolveName(args);
                return ApplyFlags(args, ResolveFlagStart(args, name), name);
            }

            private static SquirixServerCommand ApplyFlags(string[] args, int start, string name)
            {
                var state = new FlagState();
                for (var i = start; i < args.Length; i++)
                {
                    if (!TryApplyFlag(args, state, ref i))
                        return HelpCommand();
                }

                return new SquirixServerCommand(name, state.Strict, state.Uri, state.DataDirectory, state.Persist, state.SettingsPath);
            }

            private static SquirixServerCommand HelpCommand() => new("help", false, null, null, false, null);

            private static bool IsHelpFlag(string flag) => string.Equals(flag, "--help", StringComparison.Ordinal) || string.Equals(flag, "-h", StringComparison.Ordinal);

            private static string ReadValue(string[] args, ref int index)
            {
                index++;
                if (index >= args.Length || args[index].StartsWith("--", StringComparison.Ordinal))
                    throw new InvalidOperationException($"Argument '{args[index - 1]}' requires a value.");

                return args[index];
            }

            private static int ResolveFlagStart(string[] args, string name)
            {
                var isImplicitRun = string.Equals(name, "run", StringComparison.OrdinalIgnoreCase) && (args.Length == 0 || args[0].StartsWith("--", StringComparison.Ordinal));
                return isImplicitRun ? 0 : 1;
            }

            private static string ResolveName(string[] args) => args.Length == 0 || args[0].StartsWith("--", StringComparison.Ordinal) ? "run" : args[0];

            private static bool TryApplyBooleanFlag(string flag, FlagState state)
            {
                switch (flag)
                {
                    case "--strict":
                        state.SetStrict();
                        return true;
                    case "--persist":
                        state.SetPersist();
                        return true;
                    default:
                        return false;
                }
            }

            private static bool TryApplyFlag(string[] args, FlagState state, ref int index)
            {
                var flag = args[index];
                if (IsHelpFlag(flag))
                    return false;

                if (TryApplyBooleanFlag(flag, state))
                    return true;

                if (TryApplyValueFlag(args, flag, state, ref index))
                    return true;

                throw new InvalidOperationException($"Unknown argument '{flag}'.");
            }

            private static bool TryApplyValueFlag(string[] args, string flag, FlagState state, ref int index)
            {
                switch (flag)
                {
                    case "--urls":
                        state.SetUri(new Uri(ReadValue(args, ref index), UriKind.Absolute));
                        return true;
                    case "--data-dir":
                        state.SetDataDirectory(ReadValue(args, ref index));
                        return true;
                    case "--settings":
                        state.SetSettingsPath(ReadValue(args, ref index));
                        return true;
                    default:
                        return false;
                }
            }

            private sealed class FlagState
            {
                internal string? DataDirectory { get; private set; }

                internal bool Persist { get; private set; }

                internal string? SettingsPath { get; private set; }

                internal bool Strict { get; private set; }

                internal Uri? Uri { get; private set; }

                internal void SetDataDirectory(string value) => DataDirectory = value;

                internal void SetPersist() => Persist = true;

                internal void SetSettingsPath(string value) => SettingsPath = value;

                internal void SetStrict() => Strict = true;

                internal void SetUri(Uri value) => Uri = value;
            }
        }
    }
}
