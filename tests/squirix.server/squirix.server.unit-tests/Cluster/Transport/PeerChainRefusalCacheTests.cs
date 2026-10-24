using System;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for the refusal memory of <see cref="PeerChainCache" />.</summary>
[Immutable]
public sealed class PeerChainRefusalCacheTests : ServerUnitTestBase
{
    private static readonly TimeSpan MaxAge = TimeSpan.FromMinutes(10);

    /// <summary>Verifies a refusal independent of time is remembered, so a retried certificate of another authority builds no second chain.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UntrustedRefusalIsCached(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, now, out _, out var first)).IsFalse();
        _ = await Assert.That(cache.TryVerify(untrusted, now, out _, out var second)).IsFalse();

        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(1L);
        _ = await Assert.That(cache.RefusalCount).IsEqualTo(1);
        _ = await Assert.That(second).IsEqualTo(first);
        _ = await Assert.That(second.ChainStatus).IsNotEqualTo(X509ChainStatusFlags.NoError);
    }

    /// <summary>Verifies a remembered refusal is rebuilt once it is older than the maximum age.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalOlderThanMaxAgeIsRebuilt(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        clock.Advance(MaxAge);
        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(1L);
        clock.Advance(TimeSpan.FromSeconds(1));

        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies a remembered refusal is not used when the clock steps back before it was decided.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalIsRebuiltWhenClockStepsBack(CancellationToken cancellationToken)
    {
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, now, out _)).IsFalse();
        _ = await Assert.That(cache.TryVerify(untrusted, now - TimeSpan.FromMinutes(1), out _)).IsFalse();

        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies the number of remembered refusals never exceeds the capacity, however many certificates are refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CapacityBoundsRefusals(CancellationToken cancellationToken)
    {
        const int capacity = 2;
        const int refused = 7;
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        var cache = new PeerChainCache(bundle.Ca, capacity, MaxAge);

        for (var i = 0; i < refused; i++)
        {
            using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, $"node-{i}");
            _ = await Assert.That(cache.TryVerify(untrusted, now, out _)).IsFalse();
            _ = await Assert.That(cache.RefusalCount).IsLessThanOrEqualTo(capacity);
        }
    }

    /// <summary>Verifies a refusal does not evict a trusted certificate from the cache.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusalsDoNotEvictTrustedCertificates(CancellationToken cancellationToken)
    {
        const int capacity = 1;
        var now = TimeProvider.System.GetUtcNow();
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var other = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var trusted = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");
        var cache = new PeerChainCache(bundle.Ca, capacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(trusted, now, out _)).IsTrue();
        for (var i = 0; i < 3; i++)
        {
            using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(other.Ca, $"node-{i}");
            _ = await Assert.That(cache.TryVerify(untrusted, now, out _)).IsFalse();
        }

        _ = await Assert.That(cache.TryVerify(trusted, now, out _)).IsTrue();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(4L);
    }
}
