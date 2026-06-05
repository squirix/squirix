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
    /// <param name="propertyName">Property being validated.</param>
    [Theory]
    [InlineData(nameof(JournalCompactionOptions.MinTailSegments))]
    [InlineData(nameof(JournalCompactionOptions.MinTailBytes))]
    [InlineData(nameof(JournalCompactionOptions.MinGap))]
    public void FieldBackedValidationRejectsInvalidScalars(string propertyName)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => CreateWithInvalidScalar(propertyName));

        Assert.Equal(propertyName, ex.ParamName);
        Assert.Contains(propertyName, ex.Message, StringComparison.Ordinal);
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

    private static JournalCompactionOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(JournalCompactionOptions.MinTailSegments) => new JournalCompactionOptions { MinTailSegments = -1 },
        nameof(JournalCompactionOptions.MinTailBytes) => new JournalCompactionOptions { MinTailBytes = -1 },
        nameof(JournalCompactionOptions.MinGap) => new JournalCompactionOptions { MinGap = TimeSpan.FromTicks(-1) },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
