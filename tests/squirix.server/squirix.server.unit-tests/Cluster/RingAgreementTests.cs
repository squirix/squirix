using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Unit tests for <see cref="RingAgreement" />.</summary>
[Immutable]
public sealed class RingAgreementTests : ServerUnitTestBase
{
    private const int MismatchLoggedEventId = 5004;

    private static readonly string LocalValue = LocalFingerprint().Value;

    /// <summary>A new agreement is not fenced and has no mismatch.</summary>
    [Test]
    public async Task StartsNotFenced()
    {
        var agreement = Create(new EventRecordingLogger());

        agreement.EnsureNotFenced();

        _ = await Assert.That(agreement.IsFenced).IsFalse();
        _ = await Assert.That(agreement.FirstMismatch).IsNull();
    }

    /// <summary>A matching peer fingerprint passes without fencing.</summary>
    [Test]
    public async Task MatchingFingerprintPasses()
    {
        var log = new EventRecordingLogger();
        var agreement = Create(log);

        agreement.EnsureInboundAgreement(LocalValue, "n2");

        _ = await Assert.That(agreement.IsFenced).IsFalse();
        _ = await Assert.That(log.Count(MismatchLoggedEventId)).IsEqualTo(0);
    }

    /// <summary>A missing peer fingerprint fences the node and is reported as missing.</summary>
    [Test]
    public async Task MissingFingerprintFences()
    {
        var agreement = Create(new EventRecordingLogger());

        var failure = NodeExceptionAssert.For<RpcException>().Throws(agreement, static a => a.EnsureInboundAgreement(null, "n2"));

        _ = await Assert.That(RingMismatchFailure.IsMismatch(failure)).IsTrue();
        _ = await Assert.That(agreement.IsFenced).IsTrue();
        _ = await Assert.That(agreement.FirstMismatch).IsEqualTo(new RingMismatchReport("n2", RingMismatchDirection.Inbound, "missing"));
    }

    /// <summary>A different peer fingerprint fences the node, throws ring-mismatch, and logs one error.</summary>
    [Test]
    public async Task DifferentFingerprintFences()
    {
        var log = new EventRecordingLogger();
        var agreement = Create(log);
        var other = RingFingerprint.Create("cluster", ["n1", "n2", "n3"], 128).Value;

        var failure = NodeExceptionAssert.For<RpcException>().Throws((agreement, other), static s => s.agreement.EnsureInboundAgreement(s.other, "n2"));

        _ = await Assert.That(RingMismatchFailure.IsMismatch(failure)).IsTrue();
        _ = await Assert.That(agreement.IsFenced).IsTrue();
        _ = await Assert.That(agreement.FirstMismatch).IsEqualTo(new RingMismatchReport("n2", RingMismatchDirection.Inbound, other));
        _ = await Assert.That(log.Count(MismatchLoggedEventId)).IsEqualTo(1);
        _ = await Assert.That(log.Find(MismatchLoggedEventId)?.Level).IsEqualTo(LogLevel.Error);
    }

    /// <summary>An outbound mismatch fences the node with an unknown peer fingerprint.</summary>
    [Test]
    public async Task OutboundMismatchFences()
    {
        var agreement = Create(new EventRecordingLogger());

        agreement.ReportOutboundMismatch("n2");

        _ = await Assert.That(agreement.IsFenced).IsTrue();
        _ = await Assert.That(agreement.FirstMismatch).IsEqualTo(new RingMismatchReport("n2", RingMismatchDirection.Outbound, "unknown"));
    }

    /// <summary>A fenced node refuses with ring-fenced, and matching callers do not lift the fence.</summary>
    [Test]
    public async Task FenceIsSticky()
    {
        var agreement = Create(new EventRecordingLogger());
        agreement.ReportOutboundMismatch("n2");

        agreement.EnsureInboundAgreement(LocalValue, "n3");
        var failure = NodeExceptionAssert.For<RpcException>().Throws(agreement, static a => a.EnsureNotFenced());

        _ = await Assert.That(agreement.IsFenced).IsTrue();
        _ = await Assert.That(RingMismatchFailure.IsRefusal(failure)).IsTrue();
        _ = await Assert.That(RingMismatchFailure.IsMismatch(failure)).IsFalse();
    }

    /// <summary>The first mismatch is retained when later peers disagree, and each peer is logged once.</summary>
    [Test]
    public async Task FirstMismatchIsRetained()
    {
        var log = new EventRecordingLogger();
        var agreement = Create(log);

        agreement.ReportOutboundMismatch("n2");
        agreement.ReportOutboundMismatch("n2");
        agreement.ReportOutboundMismatch("n3");

        _ = await Assert.That(agreement.FirstMismatch).IsEqualTo(new RingMismatchReport("n2", RingMismatchDirection.Outbound, "unknown"));
        _ = await Assert.That(log.Count(MismatchLoggedEventId)).IsEqualTo(2);
    }

    /// <summary>Concurrent reports for one peer fence the node and log the peer exactly once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentReportsFenceOnce(CancellationToken cancellationToken)
    {
        var log = new EventRecordingLogger();
        var agreement = Create(log);
        var tasks = new Task[32];
        for (var i = 0; i < tasks.Length; i++)
            tasks[i] = StartReportAsync(agreement, cancellationToken);

        await Task.WhenAll(tasks);

        _ = await Assert.That(agreement.IsFenced).IsTrue();
        _ = await Assert.That(log.Count(MismatchLoggedEventId)).IsEqualTo(1);
    }

    private static Task StartReportAsync(RingAgreement agreement, CancellationToken cancellationToken) =>
        Task.Factory.StartNew(() => agreement.ReportOutboundMismatch("n2"), cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);

    private static RingAgreement Create(EventRecordingLogger log) => new(LocalFingerprint(), log);

    private static RingFingerprint LocalFingerprint() => RingFingerprint.Create("cluster", ["n1", "n2"], 128);
}
