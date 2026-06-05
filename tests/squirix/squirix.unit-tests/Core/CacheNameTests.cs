using System;
using Squirix.Core;
using Xunit;

namespace Squirix.UnitTests.Core;

/// <summary>
/// Tests for <see cref="CacheName" /> validation and equality semantics.
/// </summary>
public sealed class CacheNameTests : UnitTestBase
{
    /// <summary>
    /// Verifies equality and hash codes follow ordinal canonical strings.
    /// </summary>
    [Fact]
    public void EqualityAndHashCodeMatchCanonicalString()
    {
        var a = CacheName.ParsePublic("demo");
        var b = CacheName.ParsePublic("demo");
        Assert.True(a.Equals(b));
        Assert.True(a == b);
        Assert.Equal(a.GetHashCode(), b.GetHashCode());
    }

    /// <summary>
    /// Verifies unvalidated normalization maps blank names to the default cache string.
    /// </summary>
    [Fact]
    public void NormalizeUnvalidatedMapsBlankToDefaultNamespace()
    {
        Assert.Equal(CacheNames.DefaultNamespace, CacheName.NormalizeUnvalidated(null));
        Assert.Equal(CacheNames.DefaultNamespace, CacheName.NormalizeUnvalidated("   "));
    }

    /// <summary>
    /// Verifies well-formed public cache names parse to canonical values.
    /// </summary>
    [Fact]
    public void ParsePublicAcceptsValidNames()
    {
        var cn = CacheName.ParsePublic("orders-v2");
        Assert.Equal("orders-v2", cn.Canonical);
    }

    /// <summary>
    /// Verifies null and whitespace inputs fail public validation.
    /// </summary>
    [Fact]
    public void ParsePublicRejectsNullOrWhitespace()
    {
        _ = Assert.Throws<ArgumentException>(static () => CacheName.ParsePublic(null));
        _ = Assert.Throws<ArgumentException>(static () => CacheName.ParsePublic(string.Empty));
        _ = Assert.Throws<ArgumentException>(static () => CacheName.ParsePublic("   "));
    }

    /// <summary>
    /// Verifies excessive length is rejected.
    /// </summary>
    [Fact]
    public void ParsePublicRejectsTooLongNames()
    {
        var tooLong = new string('a', CacheNameValidator.MaxLength + 1);
        _ = Assert.Throws<ArgumentException>(() => CacheName.ParsePublic(tooLong));
    }

    /// <summary>
    /// Verifies string projection returns the canonical cache name.
    /// </summary>
    [Fact]
    public void ToStringReturnsCanonicalValue()
    {
        var cn = CacheName.ParsePublic("catalog");
        Assert.Equal("catalog", cn.ToString());
    }
}
