using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.ProtocolModel;

/// <summary>CLI entry point for the Raft-equivalent protocol safety explorer.</summary>
public static class Program
{
    private static readonly FrozenDictionary<string, BrokenMode> BrokenModeMap = new Dictionary<string, BrokenMode>(StringComparer.OrdinalIgnoreCase)
    {
        ["none"] = BrokenMode.None,
        ["vote"] = BrokenMode.Vote,
        ["current-term-commit"] = BrokenMode.CurrentTermCommit,
        ["read-index"] = BrokenMode.ReadIndex,
    }.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);

    /// <summary>Runs the explorer CLI.</summary>
    /// <param name="args">CLI arguments.</param>
    /// <returns>Process exit code.</returns>
    public static Task<int> Main(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        return RunAsync(args);
    }

    private static int Fail(Exception ex)
    {
        Console.Error.WriteLine("protocol-model failed: " + ex.Message);
        return 1;
    }

    private static bool IsHelpFlag(string arg) => string.Equals(arg, "-h", StringComparison.Ordinal) || string.Equals(arg, "--help", StringComparison.Ordinal);

    private static BrokenMode ParseBroken(string value)
    {
        const string message = "Expected none|vote|current-term-commit|read-index.";
        return BrokenModeMap.TryGetValue(value, out var mode) ? mode : throw new ArgumentOutOfRangeException(nameof(value), value, message);
    }

    private static string RequireValue(string[] args, string flag, ref int index)
    {
        if (index + 1 >= args.Length)
            throw new ArgumentException("Missing value for " + flag, nameof(args));

        index++;
        return args[index];
    }

    private static async Task<int> RunAsync(string[] args)
    {
        if (!TryParseArgs(args, out var options, out var showHelp))
            return 1;

        if (showHelp)
        {
            await WriteHelpAsync().ConfigureAwait(false);
            return 0;
        }

        try
        {
            return await ExploreRunner.RunCliAsync(options.Profile, options.Output, options.Broken).ConfigureAwait(false);
        }
        catch (ArgumentException ex)
        {
            return Fail(ex);
        }
        catch (InvalidOperationException ex)
        {
            return Fail(ex);
        }
        catch (IOException ex)
        {
            return Fail(ex);
        }
        catch (UnauthorizedAccessException ex)
        {
            return Fail(ex);
        }
        catch (NotSupportedException ex)
        {
            return Fail(ex);
        }
    }

    private static bool TryParseArgs(string[] args, out ArgOptions options, out bool showHelp)
    {
        options = new ArgOptions();
        showHelp = false;

        try
        {
            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (TryParseValueFlag(args, arg, ref i, options))
                    continue;

                if (!IsHelpFlag(arg))
                    throw new ArgumentException("Unrecognized argument: " + arg, nameof(args));
                showHelp = true;
                return true;
            }

            return true;
        }
        catch (ArgumentException ex)
        {
            _ = Fail(ex);
            return false;
        }
    }

    private static bool TryParseValueFlag(string[] args, string arg, ref int i, ArgOptions options)
    {
        if (string.Equals(arg, "--profile", StringComparison.Ordinal))
        {
            options.Profile = RequireValue(args, "--profile", ref i);
            return true;
        }

        if (string.Equals(arg, "--output", StringComparison.Ordinal))
        {
            options.Output = RequireValue(args, "--output", ref i);
            return true;
        }

        if (!string.Equals(arg, "--broken", StringComparison.Ordinal))
            return false;
        options.Broken = ParseBroken(RequireValue(args, "--broken", ref i));
        return true;
    }

    private static Task WriteHelpAsync()
    {
        var help = "Squirix.ProtocolModel — Raft-equivalent safety explorer\n  --profile full|small\n  --output <dir>\n" + "  --broken vote|current-term-commit|read-index|none\n" +
                   $"modelVersionHash={ExploreRunner.ModelVersionHash}\nculture={CultureInfo.InvariantCulture.Name}\n";
        return Console.Out.WriteAsync(help.AsMemory(), CancellationToken.None);
    }

    private sealed class ArgOptions
    {
        internal BrokenMode Broken { get; set; } = BrokenMode.None;

        internal string Output { get; set; } = "artifacts/protocol-model";

        internal string Profile { get; set; } = "small";
    }
}
