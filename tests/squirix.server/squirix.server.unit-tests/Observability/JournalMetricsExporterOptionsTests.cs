using System;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>
/// Unit tests for <see cref="JournalMetricsExporterOptions" /> scalar validation.
/// </summary>
public sealed class JournalMetricsExporterOptionsTests
{
    /// <summary>Verifies the minimum positive interval remains accepted.</summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryInterval()
    {
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromTicks(1) };

        Assert.Equal(TimeSpan.FromTicks(1), options.Interval);
    }

    /// <summary>Verifies non-positive intervals fail at assignment time.</summary>
    [Fact]
    public void FieldBackedValidationRejectsNonPositiveInterval()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(static () => _ = new JournalMetricsExporterOptions { Interval = TimeSpan.Zero });

        Assert.Equal("value", ex.ParamName);
        Assert.Contains(nameof(JournalMetricsExporterOptions.Interval), ex.Message, StringComparison.Ordinal);
        Assert.Contains(TimeSpan.Zero.ToString(), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through setters.</summary>
    [Fact]
    public void JsonDeserializeBindsValidatedInterval()
    {
        const string json = """{"interval":"00:00:03"}""";
        var options = new ServerJsonSerializer().Deserialize<JournalMetricsExporterOptions>(json);
        Assert.NotNull(options);
        Assert.Equal(TimeSpan.FromSeconds(3), options.Interval);
    }
}
