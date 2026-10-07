using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using Squirix.Server.Attributes;

namespace Squirix.Server.Benchmarks;

/// <summary>Minimal name and value option reader for the measurement runners.</summary>
[Immutable]
internal sealed class MeasurementArgs
{
    private readonly FrozenDictionary<string, string> _values;

    private MeasurementArgs(FrozenDictionary<string, string> values)
    {
        _values = values;
    }

    /// <summary>Parses pairs of an option name followed by its value.</summary>
    /// <param name="args">The raw arguments.</param>
    /// <returns>The parsed options.</returns>
    /// <exception cref="ArgumentException">Thrown when an option has no value.</exception>
    internal static MeasurementArgs Parse(string[] args)
    {
        ArgumentNullException.ThrowIfNull(args);
        var values = new Dictionary<string, string>(StringComparer.Ordinal);
        for (var i = 0; i < args.Length; i += 2)
        {
            if (i + 1 >= args.Length)
                throw new ArgumentException($"Option {args[i]} has no value.", nameof(args));

            values[args[i]] = args[i + 1];
        }

        return new MeasurementArgs(values.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>Reads an integer option.</summary>
    /// <param name="name">The option name.</param>
    /// <param name="fallback">The value when the option is absent.</param>
    /// <returns>The value.</returns>
    internal int GetInt(string name, int fallback) => _values.TryGetValue(name, out var text) ? int.Parse(text, CultureInfo.InvariantCulture) : fallback;

    /// <summary>Reads a comma separated integer list option.</summary>
    /// <param name="name">The option name.</param>
    /// <param name="fallback">The value when the option is absent.</param>
    /// <returns>The values.</returns>
    internal int[] GetInts(string name, int[] fallback)
    {
        if (!_values.TryGetValue(name, out var text))
            return fallback;

        var parts = text.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var result = new int[parts.Length];
        for (var i = 0; i < parts.Length; i++)
            result[i] = int.Parse(parts[i], CultureInfo.InvariantCulture);

        return result;
    }

    /// <summary>Reads a string option.</summary>
    /// <param name="name">The option name.</param>
    /// <param name="fallback">The value when the option is absent.</param>
    /// <returns>The value.</returns>
    internal string GetString(string name, string fallback) => _values.GetValueOrDefault(name, fallback);
}
