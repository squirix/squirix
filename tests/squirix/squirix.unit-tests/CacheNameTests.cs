using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Tests for <see cref="CacheName" /> validation and equality semantics.</summary>
[Immutable]
public sealed class CacheNameTests : UnitTestBase
{
    /// <summary>Verifies equality and hash codes follow ordinal canonical strings.</summary>
    [Test]
    public async Task EqualityAndHashCodeMatchCanonicalString()
    {
        var a = CacheName.ParsePublic("demo");
        var b = CacheName.ParsePublic("demo");
        _ = await Assert.That(a.Equals(b)).IsTrue();
        _ = await Assert.That(a == b).IsTrue();
        _ = await Assert.That(b.GetHashCode()).IsEqualTo(a.GetHashCode());
    }

    /// <summary>Verifies well-formed public cache names parse to canonical values.</summary>
    [Test]
    public async Task ParsePublicAcceptsValidNames()
    {
        var cn = CacheName.ParsePublic("orders-v2");
        _ = await Assert.That(cn.Canonical).IsEqualTo("orders-v2");
    }

    /// <summary>Verifies null and whitespace inputs fail public validation.</summary>
    [Test]
    public void ParsePublicRejectsNullOrWhitespace()
    {
        _ = ExceptionAssert.For<ArgumentException>().Throws(0, static _ => CacheName.ParsePublic(null));
        _ = ExceptionAssert.For<ArgumentException>().Throws(string.Empty, static value => CacheName.ParsePublic(value));
        _ = ExceptionAssert.For<ArgumentException>().Throws("   ", static value => CacheName.ParsePublic(value));
    }
}
