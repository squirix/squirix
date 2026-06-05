using System;
using Squirix.Core;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Tests for internal cache-name canonicalization and public boundary parsing.
/// </summary>
public sealed class CacheNameTests
{
    /// <summary>
    /// Verifies that public parsing validates required names and canonicalizes whitespace-only input to the default cache name.
    /// </summary>
    [Fact]
    public void PublicParseValidatesRequiredAndCanonicalizesDefault()
    {
        var ex = Assert.Throws<ArgumentException>(static () => CacheName.ParsePublic(null));
        Assert.Contains("required", ex.Message, StringComparison.OrdinalIgnoreCase);

        _ = Assert.Throws<ArgumentException>(static () => CacheName.ParsePublic("   "));
    }

    /// <summary>
    /// Verifies that trusted normalization maps null, empty, and whitespace-only names to the default cache name.
    /// </summary>
    [Fact]
    public void TrustedNormalizeMapsNullEmptyOrWhitespaceToDefault()
    {
        Assert.Equal(CacheNames.DefaultNamespace, CacheName.NormalizeUnvalidated(null));
        Assert.Equal(CacheNames.DefaultNamespace, CacheName.NormalizeUnvalidated(string.Empty));
        Assert.Equal(CacheNames.DefaultNamespace, CacheName.NormalizeUnvalidated("   "));
        Assert.Equal("orders", CacheName.NormalizeUnvalidated("orders"));
    }
}
