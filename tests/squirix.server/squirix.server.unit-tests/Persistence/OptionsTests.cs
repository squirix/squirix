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
        _ = await Assert.That(o.JournalGroupCommitMaxWait).IsEqualTo(TimeSpan.Zero);
        _ = await Assert.That(o.JournalGroupCommitMaxBatch).IsEqualTo(32);
        _ = await Assert.That(o.IsJournalGroupCommitEnabled).IsFalse();
        _ = await Assert.That(o.ReplicaLogCompactionMb).IsEqualTo(64);
        _ = await Assert.That(o.ReplicaLogCompactionEntries).IsEqualTo(100_000);
        _ = await Assert.That(o.ReplicaLogRetentionMaxMb).IsEqualTo(1024);
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
        };

        _ = await Assert.That(options.JournalMaxSegmentMb).IsEqualTo(1);
    }

    /// <summary>Verifies JSON binding still applies valid option values through init setters.</summary>
    [Test]
    public async Task JsonDeserializeBindsValidatedScalars()
    {
        const string json = """{"dataDir":"data","journalMaxSegmentMb":64,"strictFsync":true}""";
        var options = new ServerJsonSerializer().Deserialize<PersistenceOptions>(json);
        _ = await Assert.That(options).IsNotNull();
        _ = await Assert.That(options.DataDir).IsEqualTo("data");
        _ = await Assert.That(options.JournalMaxSegmentMb).IsEqualTo(64);
    }

    /// <summary>Verifies local scalar validation rejects non-positive values via <see cref="PersistenceOptions.Validate" />.</summary>
    /// <param name="propertyName">Property being validated.</param>
    [Test]
    [Arguments(nameof(PersistenceOptions.JournalMaxSegmentMb))]
    [Arguments(nameof(PersistenceOptions.JournalStallDegradedThreshold))]
    [Arguments(nameof(PersistenceOptions.ReplicaLogCompactionMb))]
    [Arguments(nameof(PersistenceOptions.ReplicaLogCompactionEntries))]
    [Arguments(nameof(PersistenceOptions.ReplicaLogRetentionMaxMb))]
    public async Task ValidateRejectsNonPositiveScalars(string propertyName)
    {
        var options = CreateWithInvalidScalar(propertyName);
        var ex = NodeExceptionAssert.For<InvalidOperationException>().Throws(options, static value => value.Validate());

        _ = await Assert.That(ex.Message).Contains(propertyName, StringComparison.Ordinal);
    }

    /// <summary>The journal segment and total sizes accept the smallest size that holds the largest frame and refuse one megabyte less.</summary>
    [Test]
    public async Task JournalSizesHoldLargestFrame()
    {
        var smallest = new PersistenceOptions { JournalMaxSegmentMb = JournalSegmentLimits.MinSegmentMb, JournalMaxTotalBytesMb = JournalSegmentLimits.MinSegmentMb };
        smallest.Validate();

        var segment = NodeExceptionAssert.For<InvalidOperationException>()
                                         .Throws(smallest with { JournalMaxSegmentMb = JournalSegmentLimits.MinSegmentMb - 1 }, static value => value.Validate());
        var total = NodeExceptionAssert.For<InvalidOperationException>()
                                       .Throws(smallest with { JournalMaxTotalBytesMb = JournalSegmentLimits.MinSegmentMb - 1 }, static value => value.Validate());

        _ = await Assert.That(segment.Message).Contains(nameof(PersistenceOptions.JournalMaxSegmentMb), StringComparison.Ordinal);
        _ = await Assert.That(total.Message).Contains(nameof(PersistenceOptions.JournalMaxTotalBytesMb), StringComparison.Ordinal);
    }

    /// <summary>The retention limit of a led group log accepts the compaction threshold and refuses one megabyte less.</summary>
    [Test]
    public async Task RetentionLimitHoldsCompactionThreshold()
    {
        var equal = new PersistenceOptions { ReplicaLogCompactionMb = 64, ReplicaLogRetentionMaxMb = 64 };
        equal.Validate();

        var below = NodeExceptionAssert.For<InvalidOperationException>().Throws(equal with { ReplicaLogRetentionMaxMb = 63 }, static value => value.Validate());

        _ = await Assert.That(below.Message).Contains(nameof(PersistenceOptions.ReplicaLogRetentionMaxMb), StringComparison.Ordinal);
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
        };

        // Overridden values
        _ = await Assert.That(overridden.DataDir).IsEqualTo("/var/lib/squirix");

        // Unchanged defaults
        _ = await Assert.That(overridden.JournalMaxSegmentMb).IsEqualTo(defaults.JournalMaxSegmentMb);
        _ = await Assert.That(overridden.JournalWriteBatch).IsEqualTo(defaults.JournalWriteBatch);
    }

    private static PersistenceOptions CreateWithInvalidScalar(string propertyName) => propertyName switch
    {
        nameof(PersistenceOptions.JournalMaxSegmentMb) => new PersistenceOptions { JournalMaxSegmentMb = 0 },
        nameof(PersistenceOptions.JournalStallDegradedThreshold) => new PersistenceOptions { JournalStallDegradedThreshold = TimeSpan.Zero },
        nameof(PersistenceOptions.ReplicaLogCompactionMb) => new PersistenceOptions { ReplicaLogCompactionMb = 0 },
        nameof(PersistenceOptions.ReplicaLogCompactionEntries) => new PersistenceOptions { ReplicaLogCompactionEntries = 0 },
        nameof(PersistenceOptions.ReplicaLogRetentionMaxMb) => new PersistenceOptions { ReplicaLogRetentionMaxMb = 0 },
        _ => throw new ArgumentOutOfRangeException(nameof(propertyName), propertyName, "Unsupported property name."),
    };
}
