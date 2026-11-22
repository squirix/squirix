using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Core;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Covers both admission limits without allocating large buffers.</summary>
public sealed class EntryPayloadSizeGuardTests
{
    /// <summary>The value limit is inclusive and excludes the encoded envelope.</summary>
    /// <param name="replicated">Whether replication is active.</param>
    /// <param name="limit">The expected value limit.</param>
    /// <param name="offset">The number of bytes above or below the limit.</param>
    /// <param name="expected">Whether the value should be admitted.</param>
    [Test]
    [Arguments(false, Frames.MaxValueBytes, -1, true)]
    [Arguments(false, Frames.MaxValueBytes, 0, true)]
    [Arguments(false, Frames.MaxValueBytes, 1, false)]
    [Arguments(true, EntryLimits.MaxReplicatedValueBytes, -1, true)]
    [Arguments(true, EntryLimits.MaxReplicatedValueBytes, 0, true)]
    [Arguments(true, EntryLimits.MaxReplicatedValueBytes, 1, false)]
    public async Task ValueLimitIsInclusive(bool replicated, int limit, int offset, bool expected)
    {
        var admission = new LargeCacheRequests(replicated, long.MaxValue);
        var valueBytes = limit + offset;
        var bodyBytes = valueBytes + 4096;
        var refusal = admission.Admit(bodyBytes, valueBytes);

        _ = await Assert.That(refusal == null).IsEqualTo(expected);
        if (refusal is { } error)
            _ = await Assert.That(error.Status).IsEqualTo(FrameStatus.ResourceExhausted);
        else
            admission.Release(bodyBytes);
    }
}
