using System;
using Squirix.Core;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Tests for centralized cache key validation.
/// </summary>
public sealed class CacheKeyValidatorTests : UnitTestBase
{
    /// <summary>
    /// Gets invalid keys and canonical messages.
    /// </summary>
    public static TheoryData<string?, string> InvalidKeys =>
        new()
        {
            { null, "Cache key is required." },
            { string.Empty, "Cache key is required." },
            { "   ", "Cache key is required." },
            { new string('a', CacheKeyValidator.MaxLength + 1), $"Cache key exceeds the maximum length of {CacheKeyValidator.MaxLength} characters." },
            { "a\u0001b", "Cache key contains control characters." },
            { "\t", "Cache key is required." },
        };

    /// <summary>
    /// Verifies max length key is accepted.
    /// </summary>
    [Fact]
    public void ValidateAcceptsMaxLengthKey()
    {
        var key = new string('x', CacheKeyValidator.MaxLength);
        Assert.Equal(key, CacheKeyValidator.Validate(key, nameof(key)));
    }

    /// <summary>
    /// Verifies common separators and Unicode are accepted.
    /// </summary>
    /// <param name="key">The key to validate.</param>
    [Theory]
    [InlineData("user:123")]
    [InlineData("tenant/acme/cart/42")]
    [InlineData("email:test@example.com")]
    [InlineData("order#123")]
    [InlineData("a/b:c.d_e-f@g#h?i=")]
    [InlineData("emoji:🔑")]
    public void ValidateAcceptsSeparatorsAndUnicode(string key) => Assert.Equal(key, CacheKeyValidator.Validate(key, nameof(key)));

    /// <summary>
    /// Verifies invalid keys fail with deterministic messages.
    /// </summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="expectedMessage">The expected canonical message.</param>
    [Theory]
    [MemberData(nameof(InvalidKeys))]
    public void ValidateRejectsInvalidKeys(string? key, string expectedMessage)
    {
        var ex = Assert.Throws<ArgumentException>(() => CacheKeyValidator.Validate(key, "key"));

        Assert.False(CacheKeyValidator.TryValidate(key, out var error));
        Assert.Equal(expectedMessage, CacheKeyValidator.GetMessage(error));
        Assert.StartsWith(expectedMessage, ex.Message, StringComparison.Ordinal);
    }
}
