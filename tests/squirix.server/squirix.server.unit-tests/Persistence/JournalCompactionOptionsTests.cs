using System;
using Squirix.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Unit tests for <see cref="JournalCompactionOptions" /> scalar validation.
/// </summary>
[Immutable]
public sealed class JournalCompactionOptionsTests
{
    /// <summary>Verifies lower-bound scalar values remain accepted.</summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryScalars()
    {
        var options = new JournalCompactionOptions
        {
            MinTailSegments = 0,
            MinTailBytes = 0,
            MinGap = TimeSpan.Zero,
        };

        Assert.Equal(0, options.MinTailSegments);
        Assert.Equal(0, options.MinTailBytes);
        Assert.Equal(TimeSpan.Zero, options.MinGap);
    }

    /// <summary>Verifies invalid scalar values fail at assignment time.</summary>
    [Fact]
    public void FieldBackedValidationRejectsInvalidScalars()
    {
        var ex = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(-1, static value => _ = new JournalCompactionOptions { MinTailSegments = value });

        Assert.Equal("value", ex.ParamName);
        Assert.Contains(nameof(JournalCompactionOptions.MinTailSegments), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through setters.</summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json = """{"enabled":true,"minTailSegments":3,"minTailBytes":4096,"minGap":"00:00:30"}""";
        var options = new ServerJsonSerializer().Deserialize<JournalCompactionOptions>(json);
        Assert.NotNull(options);
        Assert.True(options.Enabled);
        Assert.Equal(3, options.MinTailSegments);
        Assert.Equal(4096, options.MinTailBytes);
        Assert.Equal(TimeSpan.FromSeconds(30), options.MinGap);
    }
}
