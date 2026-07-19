using System;
using Squirix.Core;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Tests for <see cref="CacheName" /> validation and equality semantics.
/// </summary>
public sealed class CacheNameTests : UnitTestBase
{
    /// <summary>Verifies equality and hash codes follow ordinal canonical strings.</summary>
    [Fact]
    public void EqualityAndHashCodeMatchCanonicalString()
    {
        var a = CacheName.ParsePublic("demo");
        var b = CacheName.ParsePublic("demo");
        Assert.True(a.Equals(b));
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    /// <summary>Verifies well-formed public cache names parse to canonical values.</summary>
    [Fact]
    public void ParsePublicAcceptsValidNames()
    {
        var cn = CacheName.ParsePublic("orders-v2");
        Assert.Equal("orders-v2", cn.Canonical);
    }

    /// <summary>Verifies null and whitespace inputs fail public validation.</summary>
    [Fact]
    public void ParsePublicRejectsNullOrWhitespace()
    {
        _ = ExceptionAssert.For<ArgumentException>().Throws(0, static _ => CacheName.ParsePublic(null));
        _ = ExceptionAssert.For<ArgumentException>().Throws(string.Empty, static value => CacheName.ParsePublic(value));
        _ = ExceptionAssert.For<ArgumentException>().Throws("   ", static value => CacheName.ParsePublic(value));
    }
}
