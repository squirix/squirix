using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Unit tests for <see cref="JournalMetricsExporterOptions" /> scalar validation.</summary>
[Immutable]
public sealed class JournalMetricsExporterOptionsTests
{
    /// <summary>Verifies the minimum positive interval remains accepted.</summary>
    [Test]
    public async Task FieldValidationAcceptsBoundaryInterval()
    {
        var options = new JournalMetricsExporterOptions { Interval = TimeSpan.FromTicks(1) };

        _ = await Assert.That(options.Interval).IsEqualTo(TimeSpan.FromTicks(1));
    }

    /// <summary>Verifies non-positive intervals fail at assignment time.</summary>
    [Test]
    public async Task FieldValidationRejectsBadInterval()
    {
        var ex = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(TimeSpan.Zero, static value => _ = new JournalMetricsExporterOptions { Interval = value });

        _ = await Assert.That(ex.ParamName).IsEqualTo("value");
        _ = await Assert.That(ex.Message).Contains(nameof(JournalMetricsExporterOptions.Interval), StringComparison.Ordinal);
        _ = await Assert.That(ex.Message).Contains(TimeSpan.Zero.ToString(), StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through setters.</summary>
    [Test]
    public async Task JsonDeserializeBindsValidatedInterval()
    {
        const string json = """{"interval":"00:00:03"}""";
        var options = new ServerJsonSerializer().Deserialize<JournalMetricsExporterOptions>(json);
        _ = await Assert.That(options).IsNotNull();
        _ = await Assert.That(options.Interval).IsEqualTo(TimeSpan.FromSeconds(3));
    }
}
