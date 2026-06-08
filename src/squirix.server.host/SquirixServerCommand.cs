using System;

namespace Squirix.Server.Host;

internal sealed record SquirixServerCommand(string Name, bool Strict, string? Url, string? DataDirectory, string? SettingsPath)
{
    internal static SquirixServerCommand Parse(string[] args)
    {
        var name = args.Length == 0 || args[0].StartsWith("--", StringComparison.Ordinal) ? "run" : args[0];
        var start = name == "run" && (args.Length == 0 || args[0].StartsWith("--", StringComparison.Ordinal)) ? 0 : 1;
        var strict = false;
        string? url = null;
        string? dataDir = null;
        string? settings = null;

        for (var i = start; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "--strict":
                    strict = true;
                    break;
                case "--urls":
                    url = ReadValue(args, ref i);
                    break;
                case "--data-dir":
                    dataDir = ReadValue(args, ref i);
                    break;
                case "--settings":
                    settings = ReadValue(args, ref i);
                    break;
                case "--help":
                case "-h":
                    return new SquirixServerCommand("help", false, null, null, null);
                default:
                    throw new InvalidOperationException($"Unknown argument '{args[i]}'.");
            }
        }

        return new SquirixServerCommand(name, strict, url, dataDir, settings);
    }

    private static string ReadValue(string[] args, ref int index)
    {
        index++;
        if (index >= args.Length || args[index].StartsWith("--", StringComparison.Ordinal))
            throw new InvalidOperationException($"Argument '{args[index - 1]}' requires a value.");
        return args[index];
    }
}
