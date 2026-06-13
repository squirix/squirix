using System;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Unit tests for <see cref="CacheConflictException" />.
/// </summary>
public sealed class CacheConflictExceptionTests
{
    /// <summary>
    /// Ensures the conflicting key is exposed on the exception.
    /// </summary>
    [Fact]
    public void KeyExposesConflictingCacheKey()
    {
        var ex = new CacheConflictException("orders:42");

        Assert.Equal("orders:42", ex.Key);
        Assert.Contains("orders:42", ex.Message, StringComparison.Ordinal);
    }
}
