using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>
/// Unit tests for <see cref="PersistenceOptions" /> verifying default values,
/// record equality semantics, and behavior of <c language="csharp">with</c>-expressions.
/// </summary>
[Immutable]
public sealed class OptionsTests
{
    /// <summary>
    /// Ensures the default-constructed <see cref="PersistenceOptions" /> exposes the expected
    /// initial values for all properties.
    /// </summary>
    [Test]
    public async Task DefaultsAreExpected()
    {
        var o = new PersistenceOptions();

        _ = await Assert.That(o.DataDir).IsEqualTo(string.Empty);
        _ = await Assert.That(o.JournalMaxSegmentMb).IsEqualTo(64);
        _ = await Assert.That(o.JournalMaxSegmentCount).IsEqualTo(32);
        _ = await Assert.That(o.JournalMaxTotalBytesMb).IsEqualTo(2048);
        _ = await Assert.That(o.JournalPlatformBackend).IsEqualTo(JournalPlatformBackend.Auto);
        _ = await Assert.That(o.FlushInterval).IsEqualTo(10);
        _ = await Assert.That(o.ManifestRetentionCount).IsEqualTo(3);
        _ = await Assert.That(o.JournalGroupCommitMaxWait).IsEqualTo(TimeSpan.Zero);
        _ = await Assert.That(o.JournalGroupCommitMaxBatch).IsEqualTo(32);
        _ = await Assert.That(o.IsJournalGroupCommitEnabled).IsFalse();
    }

    /// <summary>
    /// Verifies that two default-constructed instances are value-equal and produce
    /// identical hash codes as expected for records.
    /// </summary>
    [Test]
    public async Task EqualityForDefaultsIsTrue()
    {
        var a = new PersistenceOptions();
        var b = new PersistenceOptions();

        _ = await Assert.That(b).IsEqualTo(a);
        _ = await Assert.That(b.GetHashCode()).IsEqualTo(a.GetHashCode());
    }

    /// <summary>Verifies lower-bound scalar values remain accepted.</summary>
    [Test]
    public async Task FieldValidationAcceptsValidScalars()
    {
        var options = new PersistenceOptions
        {
            JournalMaxSegmentMb = 1,
            FlushInterval = 1,
            ManifestRetentionCount = 1,
            SnapshotRetentionCount = 1,
        };

        _ = await Assert.That(options.JournalMaxSegmentMb).IsEqualTo(1);
        _ = await Assert.That(options.FlushInterval).IsEqualTo(1);
        _ = await Assert.That(options.ManifestRetentionCount).IsEqualTo(1);
        _ = await Assert.That(options.SnapshotRetentionCount).IsEqualTo(1);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Test]
    public async Task JsonDeserializeBindsValidatedScalars()
    {
        const string json = """{"dataDir":"data","journalMaxSegmentMb":64,"flushInterval":20,"manifestRetentionCount":2,"snapshotRetentionCount":4,"strictFsync":true}""";
        var options = new ServerJsonSerializer().Deserialize<PersistenceOptions>(json);
        _ = await Assert.That(options).IsNotNull();
        _ = await Assert.That(options.DataDir).IsEqualTo("data");
        _ = await Assert.That(options.JournalMaxSegmentMb).IsEqualTo(64);
        _ = await Assert.That(options.FlushInterval).IsEqualTo(20);
        _ = await Assert.That(options.ManifestRetentionCount).IsEqualTo(2);
        _ = await Assert.That(options.SnapshotRetentionCount).IsEqualTo(4);
    }

    /// <summary>Verifies local scalar validation rejects non-positive values via <see cref="PersistenceOptions.Validate" />.</summary>
    /// <param name="propertyName">Property being validated.</param>
    [Test]
    [Arguments(nameof(PersistenceOptions.JournalMaxSegmentMb))]
    [Arguments(nameof(PersistenceOptions.FlushInterval))]
    [Arguments(nameof(PersistenceOptions.ManifestRetentionCount))]
    [Arguments(nameof(PersistenceOptions.SnapshotRetentionCount))]
    public async Task ValidateRejectsNonPositiveScalars(string propertyName)
    {
        var options = CreateWithInvalidScalar(propertyName);
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains(propertyName, StringComparison.Ordinal);
    }

    /// <summary>
    /// Checks that using a <c language="csharp">with</c>-expression overrides only the specified properties
    /// while leaving all other properties unchanged from the source instance.
    /// </summary>
    [Test]
    public async Task WithOverridesSelectedPropertiesOnly()
    {
        var defaults = new PersistenceOptions();

        var overridden = defaults with
        {
            DataDir = "/var/lib/squirix",
            ManifestRetentionCount = 100,
        };

        // Overridden values
        _ = await Assert.That(overridden.DataDir).IsEqualTo("/var/lib/squirix");
        _ = await Assert.That(overridden.ManifestRetentionCount).IsEqualTo(100);

        // Unchanged defaults
        _ = await Assert.That(overridden.JournalMaxSegmentMb).IsEqualTo(defaults.JournalMaxSegmentMb);
        _ = await Assert.That(overridden.FlushInterval).IsEqualTo(defaults.FlushInterval);
    }

    private static PersistenceOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(PersistenceOptions.JournalMaxSegmentMb) => new PersistenceOptions { JournalMaxSegmentMb = 0 },
        nameof(PersistenceOptions.FlushInterval) => new PersistenceOptions { FlushInterval = 0 },
        nameof(PersistenceOptions.ManifestRetentionCount) => new PersistenceOptions { ManifestRetentionCount = 0 },
        nameof(PersistenceOptions.SnapshotRetentionCount) => new PersistenceOptions { SnapshotRetentionCount = 0 },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
