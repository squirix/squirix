using System;
using System.Security.Cryptography.X509Certificates;
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
    [Test]
    public async Task UntrustedRefusalIsCached()
    {
        var now = TimeProvider.System.GetUtcNow();
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var otherAuthority = MtlsTestCertificateFactory.CreateAuthority();
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-b");
        var cache = new PeerChainCache(authority, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, now, out _, out var first)).IsFalse();
        _ = await Assert.That(cache.TryVerify(untrusted, now, out _, out var second)).IsFalse();

        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(1L);
        _ = await Assert.That(cache.RefusalCount).IsEqualTo(1);
        _ = await Assert.That(second).IsEqualTo(first);
        _ = await Assert.That(second.ChainStatus).IsNotEqualTo(X509ChainStatusFlags.NoError);
    }

    /// <summary>Verifies a remembered refusal is rebuilt once it is older than the maximum age.</summary>
    [Test]
    public async Task RefusalOlderThanMaxAgeIsRebuilt()
    {
        var clock = new FakeTimeProvider(TimeProvider.System.GetUtcNow());
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var otherAuthority = MtlsTestCertificateFactory.CreateAuthority();
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-b");
        var cache = new PeerChainCache(authority, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        clock.Advance(MaxAge);
        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(1L);
        clock.Advance(TimeSpan.FromSeconds(1));

        _ = await Assert.That(cache.TryVerify(untrusted, clock.GetUtcNow(), out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies a remembered refusal is not used when the clock steps back before it was decided.</summary>
    [Test]
    public async Task RefusalIsRebuiltWhenClockStepsBack()
    {
        var now = TimeProvider.System.GetUtcNow();
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var otherAuthority = MtlsTestCertificateFactory.CreateAuthority();
        using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-b");
        var cache = new PeerChainCache(authority, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(untrusted, now, out _)).IsFalse();
        _ = await Assert.That(cache.TryVerify(untrusted, now - TimeSpan.FromMinutes(1), out _)).IsFalse();

        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies the number of remembered refusals never exceeds the capacity, however many certificates are refused.</summary>
    [Test]
    public async Task CapacityBoundsRefusals()
    {
        const int capacity = 2;
        var now = TimeProvider.System.GetUtcNow();
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var otherAuthority = MtlsTestCertificateFactory.CreateAuthority();
        var cache = new PeerChainCache(authority, capacity, MaxAge);

        using var first = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-a");
        using var second = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-b");
        using var third = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-c");

        _ = await Assert.That(cache.TryVerify(first, now, out _)).IsFalse();
        _ = await Assert.That(cache.TryVerify(second, now, out _)).IsFalse();
        _ = await Assert.That(cache.TryVerify(third, now, out _)).IsFalse();
        _ = await Assert.That(cache.RefusalCount).IsEqualTo(capacity);
        _ = await Assert.That(cache.TryVerify(second, now, out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(3L);

        _ = await Assert.That(cache.TryVerify(first, now, out _)).IsFalse();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(4L);
    }

    /// <summary>Verifies a refusal does not evict a trusted certificate from the cache.</summary>
    [Test]
    public async Task RefusalsDoNotEvictTrustedCertificates()
    {
        const int capacity = 1;
        var now = TimeProvider.System.GetUtcNow();
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var otherAuthority = MtlsTestCertificateFactory.CreateAuthority();
        using var trusted = MtlsTestCertificateFactory.CreatePeerCertificate(authority, "node-b");
        var cache = new PeerChainCache(authority, capacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(trusted, now, out _)).IsTrue();
        for (var i = 0; i < 3; i++)
        {
            using var untrusted = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, $"node-{i}");
            _ = await Assert.That(cache.TryVerify(untrusted, now, out _)).IsFalse();
        }

        _ = await Assert.That(cache.TryVerify(trusted, now, out _)).IsTrue();
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(4L);
    }

    /// <summary>Verifies an expired certificate of another authority, whose status mixes time and trust failures, is not remembered.</summary>
    [Test]
    public async Task ExpiredOtherAuthorityIsNotRemembered()
    {
        var now = TimeProvider.System.GetUtcNow();
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var otherAuthority = MtlsTestCertificateFactory.CreateAuthority();
        using var expired = MtlsTestCertificateFactory.CreatePeerCertificate(otherAuthority, "node-b", now.AddHours(-2), now.AddHours(-1));
        var cache = new PeerChainCache(authority, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(expired, now, out _, out var refusal)).IsFalse();
        _ = await Assert.That(cache.TryVerify(expired, now, out _)).IsFalse();

        _ = await Assert.That((refusal.ChainStatus & X509ChainStatusFlags.NotTimeValid) == X509ChainStatusFlags.NotTimeValid).IsTrue();
        _ = await Assert.That(refusal.ChainStatus).IsNotEqualTo(X509ChainStatusFlags.NotTimeValid);
        _ = await Assert.That(cache.RefusalCount).IsEqualTo(0);
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }

    /// <summary>Verifies an expired certificate of the cluster authority, which fails on time alone, is not remembered.</summary>
    [Test]
    public async Task ExpiredClusterCertIsNotRemembered()
    {
        var now = TimeProvider.System.GetUtcNow();
        using var authority = MtlsTestCertificateFactory.CreateAuthority();
        using var expired = MtlsTestCertificateFactory.CreatePeerCertificate(authority, "node-b", now.AddHours(-2), now.AddHours(-1));
        var cache = new PeerChainCache(authority, PeerChainCache.DefaultCapacity, MaxAge);

        _ = await Assert.That(cache.TryVerify(expired, now, out _, out var refusal)).IsFalse();
        _ = await Assert.That(cache.TryVerify(expired, now, out _)).IsFalse();

        _ = await Assert.That(refusal.ChainStatus).IsEqualTo(X509ChainStatusFlags.NotTimeValid);
        _ = await Assert.That(cache.RefusalCount).IsEqualTo(0);
        _ = await Assert.That(cache.ChainBuilds).IsEqualTo(2L);
    }
}
