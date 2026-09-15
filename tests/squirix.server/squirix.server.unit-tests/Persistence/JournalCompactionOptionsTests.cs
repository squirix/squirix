using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Unit tests for <see cref="JournalCompactionOptions" /> scalar validation.</summary>
[Immutable]
public sealed class JournalCompactionOptionsTests
{
    /// <summary>Verifies lower-bound scalar values remain accepted.</summary>
    [Test]
    public async Task FieldValidationAcceptsValidScalars()
    {
        var options = new JournalCompactionOptions
        {
            MinTailSegments = 0,
            MinTailBytes = 0,
            MinGap = TimeSpan.Zero,
        };

        _ = await Assert.That(options.MinTailSegments).IsEqualTo(0);
        _ = await Assert.That(options.MinTailBytes).IsEqualTo(0);
        _ = await Assert.That(options.MinGap).IsEqualTo(TimeSpan.Zero);
    }

    /// <summary>Verifies invalid scalar values fail at assignment time.</summary>
    [Test]
    public async Task FieldValidationRejectsBadScalars()
    {
        var ex = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(-1, static value => _ = new JournalCompactionOptions { MinTailSegments = value });

        _ = await Assert.That(ex.ParamName).IsEqualTo("value");
        _ = await Assert.That(ex.Message).Contains(nameof(JournalCompactionOptions.MinTailSegments), StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through setters.</summary>
    [Test]
    public async Task JsonDeserializeBindsValidatedScalars()
    {
        const string json = """{"enabled":true,"minTailSegments":3,"minTailBytes":4096,"minGap":"00:00:30"}""";
        var options = new ServerJsonSerializer().Deserialize<JournalCompactionOptions>(json);
        _ = await Assert.That(options).IsNotNull();
        _ = await Assert.That(options.Enabled).IsTrue();
        _ = await Assert.That(options.MinTailSegments).IsEqualTo(3);
        _ = await Assert.That(options.MinTailBytes).IsEqualTo(4096);
        _ = await Assert.That(options.MinGap).IsEqualTo(TimeSpan.FromSeconds(30));
    }
}
