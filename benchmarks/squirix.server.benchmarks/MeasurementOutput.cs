using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.InteropServices;

namespace Squirix.Server.Benchmarks;

/// <summary>Console and Markdown output helpers shared by the measurement runners.</summary>
internal static class MeasurementOutput
{
    /// <summary>Formats a number with two decimals and the invariant culture.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The text.</returns>
    internal static string Fixed2(double value) => value.ToString("F2", CultureInfo.InvariantCulture);

    /// <summary>Formats a number without decimals and with the invariant culture.</summary>
    /// <param name="value">The value.</param>
    /// <returns>The text.</returns>
    internal static string Fixed0(double value) => value.ToString("F0", CultureInfo.InvariantCulture);

    /// <summary>Formats milliseconds as seconds with two decimals, or n/a for NaN.</summary>
    /// <param name="milliseconds">The duration in milliseconds.</param>
    /// <returns>The text.</returns>
    internal static string Seconds(double milliseconds) => double.IsNaN(milliseconds) ? "n/a" : Fixed2(milliseconds / 1000.0);

    /// <summary>Writes one line to the console.</summary>
    /// <param name="row">The line.</param>
    internal static void Line(string row) => Console.Out.WriteLine(row);

    /// <summary>Writes a Markdown table.</summary>
    /// <param name="title">The heading text.</param>
    /// <param name="header">The pipe-separated column titles, with leading and trailing pipes.</param>
    /// <param name="rows">The rows, each already formatted with leading and trailing pipes.</param>
    internal static void Table(string title, string header, List<string> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        Line(string.Empty);
        Line("### " + title);
        Line(string.Empty);
        Line(header);
        Line("|" + string.Concat(Repeat("---|", header.Split('|', StringSplitOptions.RemoveEmptyEntries).Length)));
        foreach (var row in CollectionsMarshal.AsSpan(rows))
            Line(row);
    }

    private static string[] Repeat(string value, int count)
    {
        var result = new string[count];
        Array.Fill(result, value);
        return result;
    }
}
