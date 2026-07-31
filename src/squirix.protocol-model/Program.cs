using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.ProtocolModel;

/// <summary>CLI entry point for the Raft-equivalent protocol safety explorer.</summary>
public static class Program
{
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

    private static BrokenMode ParseBroken(string value)
    {
        if (string.Equals(value, "none", StringComparison.OrdinalIgnoreCase))
            return BrokenMode.None;

        if (string.Equals(value, "vote", StringComparison.OrdinalIgnoreCase))
            return BrokenMode.Vote;

        if (string.Equals(value, "current-term-commit", StringComparison.OrdinalIgnoreCase))
            return BrokenMode.CurrentTermCommit;

        if (string.Equals(value, "read-index", StringComparison.OrdinalIgnoreCase))
            return BrokenMode.ReadIndex;

        throw new ArgumentOutOfRangeException(nameof(value), value, "Expected none|vote|current-term-commit|read-index.");
    }

    private static string RequireValue(string[] args, ref int index, string flag)
    {
        if (index + 1 >= args.Length)
            throw new ArgumentException("Missing value for " + flag, nameof(args));

        index++;
        return args[index];
    }

    private static async Task<int> RunAsync(string[] args)
    {
        if (!TryParseArgs(args, out var profile, out var output, out var broken, out var showHelp))
            return 1;

        if (showHelp)
        {
            await WriteHelpAsync().ConfigureAwait(false);
            return 0;
        }

        try
        {
            return await ExploreRunner.RunCliAsync(profile, output, broken).ConfigureAwait(false);
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

    private static bool TryParseArgs(string[] args, out string profile, out string output, out BrokenMode broken, out bool showHelp)
    {
        profile = "small";
        output = "artifacts/protocol-model";
        broken = BrokenMode.None;
        showHelp = false;

        try
        {
            for (var i = 0; i < args.Length; i++)
            {
                var arg = args[i];
                if (string.Equals(arg, "--profile", StringComparison.Ordinal))
                {
                    profile = RequireValue(args, ref i, "--profile");
                    continue;
                }

                if (string.Equals(arg, "--output", StringComparison.Ordinal))
                {
                    output = RequireValue(args, ref i, "--output");
                    continue;
                }

                if (string.Equals(arg, "--broken", StringComparison.Ordinal))
                {
                    broken = ParseBroken(RequireValue(args, ref i, "--broken"));
                    continue;
                }

                if (!string.Equals(arg, "-h", StringComparison.Ordinal) && !string.Equals(arg, "--help", StringComparison.Ordinal))
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

    private static Task WriteHelpAsync()
    {
        var help = "Squirix.ProtocolModel — Raft-equivalent safety explorer\n  --profile full|small\n  --output <dir>\n" + "  --broken vote|current-term-commit|read-index|none\n" +
                   $"modelVersionHash={ExploreRunner.ModelVersionHash}\nculture={CultureInfo.InvariantCulture.Name}\n";
        return Console.Out.WriteAsync(help.AsMemory(), CancellationToken.None);
    }
}
