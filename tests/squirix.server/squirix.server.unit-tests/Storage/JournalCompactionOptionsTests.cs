using System;
using System.Text.Json;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Unit tests for <see cref="JournalCompactionOptions" /> scalar validation.
/// </summary>
public sealed class JournalCompactionOptionsTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Verifies lower-bound scalar values remain accepted.
    /// </summary>
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

    /// <summary>
    /// Verifies invalid scalar values fail at assignment time.
    /// </summary>
    [Fact]
    public void FieldBackedValidationRejectsInvalidScalars()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(static () => new JournalCompactionOptions { MinTailSegments = -1 });

        Assert.Equal("value", ex.ParamName);
        Assert.Contains(nameof(JournalCompactionOptions.MinTailSegments), ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies JSON binding still applies valid option values through setters.
    /// </summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json = """
                            {
                              "enabled": true,
                              "minTailSegments": 3,
                              "minTailBytes": 4096,
                              "minGap": "00:00:30"
                            }
                            """;

        var options = JsonSerializer.Deserialize<JournalCompactionOptions>(json, JsonOptions);

        Assert.NotNull(options);
        Assert.True(options.Enabled);
        Assert.Equal(3, options.MinTailSegments);
        Assert.Equal(4096, options.MinTailBytes);
        Assert.Equal(TimeSpan.FromSeconds(30), options.MinGap);
    }
}
