using System;
using System.Text.Json;
using Squirix.Server.Node.Services;
using Xunit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// Unit tests for <see cref="JournalMetricsExporterOptions" /> scalar validation.
/// </summary>
public sealed class JournalMetricsExporterOptionsTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Verifies the minimum positive interval remains accepted.
    /// </summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryInterval()
    {
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromTicks(1) };

        Assert.Equal(TimeSpan.FromTicks(1), options.Interval);
    }

    /// <summary>
    /// Verifies non-positive intervals fail at assignment time.
    /// </summary>
    [Fact]
    public void FieldBackedValidationRejectsNonPositiveInterval()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(static () => new JournalMetricsExporterOptions { Interval = TimeSpan.Zero });

        Assert.Equal(nameof(JournalMetricsExporterOptions.Interval), ex.ParamName);
        Assert.Contains(nameof(JournalMetricsExporterOptions.Interval), ex.Message, StringComparison.Ordinal);
        Assert.Contains(TimeSpan.Zero.ToString(), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies JSON binding still applies valid option values through setters.
    /// </summary>
    [Fact]
    public void JsonDeserializeBindsValidatedInterval()
    {
        const string json = """
                            {
                              "interval": "00:00:03"
                            }
                            """;

        var options = JsonSerializer.Deserialize<JournalMetricsExporterOptions>(json, JsonOptions);

        Assert.NotNull(options);
        Assert.Equal(TimeSpan.FromSeconds(3), options.Interval);
    }
}
