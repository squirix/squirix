using System;
using System.Text.Json;
using Squirix.Server.Storage;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Unit tests for <see cref="PersistenceOptions" /> verifying default values,
/// record equality semantics, and behavior of <c>with</c>-expressions.
/// </summary>
public sealed class PersistenceOptionsTests
{
    private static readonly JsonSerializerOptions JsonOptions = new()
    {
        PropertyNameCaseInsensitive = true,
    };

    /// <summary>
    /// Ensures the default-constructed <see cref="PersistenceOptions" /> exposes the expected
    /// initial values for all properties.
    /// </summary>
    [Fact]
    public void DefaultsAreExpected()
    {
        var o = new PersistenceOptions();

        Assert.Equal(string.Empty, o.DataDir);
        Assert.Equal(128, o.JournalMaxSegmentMb);
        Assert.Equal(10, o.FlushIntervalMs);
        Assert.Equal(60, o.SnapshotIntervalSec);
        Assert.Equal(3, o.ManifestRetentionCount);
        Assert.False(o.StrictFsync);
    }

    /// <summary>
    /// Verifies that two default-constructed instances are value-equal and produce
    /// identical hash codes as expected for records.
    /// </summary>
    [Fact]
    public void EqualityForDefaultsIsTrue()
    {
        var a = new PersistenceOptions();
        var b = new PersistenceOptions();

        Assert.Equal(a, b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    /// <summary>
    /// Verifies lower-bound scalar values remain accepted.
    /// </summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryScalars()
    {
        var options = new PersistenceOptions
        {
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 1,
            SnapshotIntervalSec = 1,
            ManifestRetentionCount = 1,
            SnapshotRetentionCount = 1,
        };

        Assert.Equal(1, options.JournalMaxSegmentMb);
        Assert.Equal(1, options.FlushIntervalMs);
        Assert.Equal(1, options.SnapshotIntervalSec);
        Assert.Equal(1, options.ManifestRetentionCount);
        Assert.Equal(1, options.SnapshotRetentionCount);
    }

    /// <summary>
    /// Verifies local scalar validation rejects non-positive values at assignment time.
    /// </summary>
    /// <param name="propertyName">Property being validated.</param>
    [Theory]
    [InlineData(nameof(PersistenceOptions.JournalMaxSegmentMb))]
    [InlineData(nameof(PersistenceOptions.FlushIntervalMs))]
    [InlineData(nameof(PersistenceOptions.SnapshotIntervalSec))]
    [InlineData(nameof(PersistenceOptions.ManifestRetentionCount))]
    [InlineData(nameof(PersistenceOptions.SnapshotRetentionCount))]
    public void FieldBackedValidationRejectsNonPositiveScalars(string propertyName)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => CreateWithInvalidScalar(propertyName));

        Assert.Equal(propertyName, ex.ParamName);
        Assert.Contains(propertyName, ex.Message, StringComparison.Ordinal);
        Assert.Contains("0", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>
    /// Verifies JSON binding still applies valid option values through init setters.
    /// </summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json = """
                            {
                              "dataDir": "data",
                              "journalMaxSegmentMb": 64,
                              "flushIntervalMs": 20,
                              "snapshotIntervalSec": 30,
                              "manifestRetentionCount": 2,
                              "snapshotRetentionCount": 4,
                              "strictFsync": true
                            }
                            """;

        var options = JsonSerializer.Deserialize<PersistenceOptions>(json, JsonOptions);

        Assert.NotNull(options);
        Assert.Equal("data", options.DataDir);
        Assert.Equal(64, options.JournalMaxSegmentMb);
        Assert.Equal(20, options.FlushIntervalMs);
        Assert.Equal(30, options.SnapshotIntervalSec);
        Assert.Equal(2, options.ManifestRetentionCount);
        Assert.Equal(4, options.SnapshotRetentionCount);
        Assert.True(options.StrictFsync);
    }

    /// <summary>
    /// Checks that using a <c>with</c>-expression overrides only the specified properties
    /// while leaving all other properties unchanged from the source instance.
    /// </summary>
    [Fact]
    public void WithExpressionOverridesSelectedPropertiesOnly()
    {
        var defaults = new PersistenceOptions();

        var overridden = defaults with
        {
            DataDir = "/var/lib/squirix",
            StrictFsync = true,
            ManifestRetentionCount = 100,
        };

        // Overridden values
        Assert.Equal("/var/lib/squirix", overridden.DataDir);
        Assert.Equal(100, overridden.ManifestRetentionCount);
        Assert.True(overridden.StrictFsync);

        // Unchanged defaults
        Assert.Equal(defaults.JournalMaxSegmentMb, overridden.JournalMaxSegmentMb);
        Assert.Equal(defaults.FlushIntervalMs, overridden.FlushIntervalMs);
        Assert.Equal(defaults.SnapshotIntervalSec, overridden.SnapshotIntervalSec);
    }

    private static PersistenceOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(PersistenceOptions.JournalMaxSegmentMb) => new PersistenceOptions { JournalMaxSegmentMb = 0 },
        nameof(PersistenceOptions.FlushIntervalMs) => new PersistenceOptions { FlushIntervalMs = 0 },
        nameof(PersistenceOptions.SnapshotIntervalSec) => new PersistenceOptions { SnapshotIntervalSec = 0 },
        nameof(PersistenceOptions.ManifestRetentionCount) => new PersistenceOptions { ManifestRetentionCount = 0 },
        nameof(PersistenceOptions.SnapshotRetentionCount) => new PersistenceOptions { SnapshotRetentionCount = 0 },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
