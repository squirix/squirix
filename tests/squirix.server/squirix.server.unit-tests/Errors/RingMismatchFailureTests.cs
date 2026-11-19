using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Errors;

/// <summary>Unit tests for <see cref="RingMismatchFailure" />.</summary>
[Immutable]
public sealed class RingMismatchFailureTests : ServerUnitTestBase
{
    private const string ErrorCodeKey = "squirix-error-code";

    /// <summary>The mismatch failure is Unavailable with the ring-mismatch trailer and stable detail.</summary>
    [Test]
    public async Task MismatchCarriesStatusTrailerAndDetail()
    {
        var failure = RingMismatchFailure.Mismatch();

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(failure.FindTrailer(ErrorCodeKey)).IsEqualTo("ring-mismatch");
        _ = await Assert.That(failure.Message).IsEqualTo(
            "Cluster ring mismatch: the forwarding node and the key owner disagree on the peer list, ring settings or server version; nothing was executed.");
    }

    /// <summary>The fenced failure is Unavailable with the ring-fenced trailer and stable detail.</summary>
    [Test]
    public async Task FencedCarriesStatusTrailerAndDetail()
    {
        var failure = RingMismatchFailure.Fenced();

        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(failure.FindTrailer(ErrorCodeKey)).IsEqualTo("ring-fenced");
        _ = await Assert.That(failure.Message).IsEqualTo(
            "Cache operations are refused: the key owner or this node detected a cluster ring mismatch with a peer; make the peer lists agree and restart the affected nodes.");
    }

    /// <summary>Only the mismatch failure is classified as a mismatch.</summary>
    [Test]
    public async Task ClassifiesMismatch()
    {
        _ = await Assert.That(RingMismatchFailure.IsMismatch(RingMismatchFailure.Mismatch())).IsTrue();
        _ = await Assert.That(RingMismatchFailure.IsMismatch(RingMismatchFailure.Fenced())).IsFalse();
    }

    /// <summary>Both ring failures are refusals.</summary>
    [Test]
    public async Task ClassifiesRefusals()
    {
        _ = await Assert.That(RingMismatchFailure.IsRefusal(RingMismatchFailure.Mismatch())).IsTrue();
        _ = await Assert.That(RingMismatchFailure.IsRefusal(RingMismatchFailure.Fenced())).IsTrue();
    }

    /// <summary>Stale-owner and plain Unavailable failures are not ring refusals.</summary>
    [Test]
    public async Task ExcludesOtherFailures()
    {
        var staleOwner = StaleOwnerFailure.Create("n2", "n1");
        var plain = new ServerCallException(FrameStatus.Unavailable, "down");
        var wrongStatus = new ServerCallException(FrameStatus.Internal, RingMismatchFailure.Mismatch().Error);

        _ = await Assert.That(RingMismatchFailure.IsRefusal(staleOwner)).IsFalse();
        _ = await Assert.That(RingMismatchFailure.IsRefusal(plain)).IsFalse();
        _ = await Assert.That(RingMismatchFailure.IsRefusal(wrongStatus)).IsFalse();
        _ = await Assert.That(RingMismatchFailure.IsMismatch(plain)).IsFalse();
    }
}
