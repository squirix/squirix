using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Journal sizing agrees with materialized entries for every deadline shape.</summary>
public sealed class EntrySizingTests
{
    /// <summary>Relative expiration reserves the same deadline bytes as journal materialization.</summary>
    /// <param name="absolute">Whether an absolute deadline is present.</param>
    /// <param name="relative">Whether a relative expiration is present.</param>
    /// <returns>The assertion operation.</returns>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task LengthMatchesPrepared(bool absolute, bool relative)
    {
        var deadline = new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var entry = new NodeCacheEntry<object?>("value", 1, absolute ? deadline : null, relative ? TimeSpan.FromMinutes(1) : null);
        var materialized = new NodeCacheEntry<object?>("value", 1, absolute || relative ? deadline : null);

        _ = await Assert.That(PreparedJournalEntry.MeasureEncodedLength(entry)).IsEqualTo(PreparedJournalEntry.From(materialized).EncodedLength);
    }
}
