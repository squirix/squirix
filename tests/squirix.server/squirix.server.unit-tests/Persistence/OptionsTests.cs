using System;
using Squirix.Server.Serialization;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Unit tests for <see cref="PersistenceOptions" /> verifying default values,
/// record equality semantics, and behavior of <c>with</c>-expressions.
/// </summary>
public sealed class OptionsTests
{
    /// <summary>
    /// Ensures the default-constructed <see cref="PersistenceOptions" /> exposes the expected
    /// initial values for all properties.
    /// </summary>
    [Fact]
    public void DefaultsAreExpected()
    {
        var o = new PersistenceOptions();

        Assert.Equal(string.Empty, o.DataDir);
        Assert.Equal(64, o.JournalMaxSegmentMb);
        Assert.Equal(32, o.JournalMaxSegmentCount);
        Assert.Equal(2048, o.JournalMaxTotalBytesMb);
        Assert.Equal(JournalPlatformBackend.Auto, o.JournalPlatformBackend);
        Assert.Equal(10, o.FlushIntervalMs);
        Assert.Equal(3, o.ManifestRetentionCount);
        Assert.Equal(TimeSpan.Zero, o.JournalGroupCommitMaxWait);
        Assert.Equal(32, o.JournalGroupCommitMaxBatch);
        Assert.False(o.IsJournalGroupCommitEnabled);
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

    /// <summary>Verifies lower-bound scalar values remain accepted.</summary>
    [Fact]
    public void FieldBackedValidationAcceptsBoundaryScalars()
    {
        var options = new PersistenceOptions
        {
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 1,
            ManifestRetentionCount = 1,
            SnapshotRetentionCount = 1,
        };

        Assert.Equal(1, options.JournalMaxSegmentMb);
        Assert.Equal(1, options.FlushIntervalMs);
        Assert.Equal(1, options.ManifestRetentionCount);
        Assert.Equal(1, options.SnapshotRetentionCount);
    }

    /// <summary>Verifies local scalar validation rejects non-positive values at assignment time.</summary>
    /// <param name="propertyName">Property being validated.</param>
    [Theory]
    [InlineData(nameof(PersistenceOptions.JournalMaxSegmentMb))]
    [InlineData(nameof(PersistenceOptions.FlushIntervalMs))]
    [InlineData(nameof(PersistenceOptions.SnapshotIntervalSec))]
    [InlineData(nameof(PersistenceOptions.ManifestRetentionCount))]
    [InlineData(nameof(PersistenceOptions.SnapshotRetentionCount))]
    public void FieldBackedValidationRejectsNonPositiveScalars(string propertyName)
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => _ = CreateWithInvalidScalar(propertyName));

        Assert.Equal("value", ex.ParamName);
        Assert.Contains(propertyName, ex.Message, StringComparison.Ordinal);
        Assert.Contains("0", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Fact]
    public void JsonDeserializeBindsValidatedScalars()
    {
        const string json =
            """{"dataDir":"data","journalMaxSegmentMb":64,"flushIntervalMs":20,"snapshotIntervalSec":30,"manifestRetentionCount":2,"snapshotRetentionCount":4,"strictFsync":true}""";
        var options = new SystemTextJsonSerializer().Deserialize<PersistenceOptions>(json);
        Assert.NotNull(options);
        Assert.Equal("data", options.DataDir);
        Assert.Equal(64, options.JournalMaxSegmentMb);
        Assert.Equal(20, options.FlushIntervalMs);
        Assert.Equal(2, options.ManifestRetentionCount);
        Assert.Equal(4, options.SnapshotRetentionCount);
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
            ManifestRetentionCount = 100,
        };

        // Overridden values
        Assert.Equal("/var/lib/squirix", overridden.DataDir);
        Assert.Equal(100, overridden.ManifestRetentionCount);

        // Unchanged defaults
        Assert.Equal(defaults.JournalMaxSegmentMb, overridden.JournalMaxSegmentMb);
        Assert.Equal(defaults.FlushIntervalMs, overridden.FlushIntervalMs);
    }

    private static PersistenceOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(PersistenceOptions.JournalMaxSegmentMb) => new PersistenceOptions { JournalMaxSegmentMb = 0 },
        nameof(PersistenceOptions.FlushIntervalMs) => new PersistenceOptions { FlushIntervalMs = 0 },
        nameof(PersistenceOptions.ManifestRetentionCount) => new PersistenceOptions { ManifestRetentionCount = 0 },
        nameof(PersistenceOptions.SnapshotRetentionCount) => new PersistenceOptions { SnapshotRetentionCount = 0 },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
