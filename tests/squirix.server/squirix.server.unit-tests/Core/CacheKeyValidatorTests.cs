using System;
using System.Runtime.CompilerServices;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Covers cache key validation and display formatting.</summary>
[Immutable]
public sealed class CacheKeyValidatorTests : ServerUnitTestBase
{
    /// <summary>CacheKey formats default and namespaced keys.</summary>
    [Fact]
    public void CacheKeyToStringFormatsNamespaceAndKey()
    {
        Assert.Equal("plain", new CacheKey(string.Empty, "plain").ToString());
        Assert.Equal("ns:plain", new CacheKey("ns", "plain").ToString());
        Assert.Equal(ServerCacheNames.DefaultNamespace + ":k", CacheKey.Default("k").ToString());
    }

    /// <summary>GetMessage rejects unknown enum values.</summary>
    [Fact]
    public void GetMessageRejectsUnknownError()
    {
        var raw = 42;
        var error = Unsafe.As<int, ServerKeyValidationError>(ref raw);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(error, static value => _ = CacheKeyValidator.GetMessage(value));
    }

    /// <summary>Accepts ordinary keys and returns stable required/control/too-long diagnostics.</summary>
    [Fact]
    public void ValidateCoversSuccessAndFailurePaths()
    {
        Assert.True(CacheKeyValidator.TryValidate("ok", out var ok));
        Assert.Equal(default, ok);
        Assert.Equal("ok", CacheKeyValidator.Validate("ok", "key"));

        Assert.False(CacheKeyValidator.TryValidate(null, out var required));
        Assert.Equal(ServerKeyValidationError.Required, required);
        Assert.Equal("Cache key is required.", CacheKeyValidator.GetMessage(required));

        Assert.False(CacheKeyValidator.TryValidate("   ", out var whitespace));
        Assert.Equal(ServerKeyValidationError.Required, whitespace);

        Assert.False(CacheKeyValidator.TryValidate("a\tb", out var control));
        Assert.Equal(ServerKeyValidationError.ControlCharacters, control);
        Assert.Equal("Cache key contains control characters.", CacheKeyValidator.GetMessage(control));

        var tooLong = new string('k', 1025);
        Assert.False(CacheKeyValidator.TryValidate(tooLong, out var length));
        Assert.Equal(ServerKeyValidationError.TooLong, length);
        Assert.Equal("Cache key exceeds the maximum length of 1024 characters.", CacheKeyValidator.GetMessage(length));

        var max = new string('k', 1024);
        Assert.True(CacheKeyValidator.TryValidate(max, out _));
    }

    /// <summary>Validate throws ArgumentException with the caller parameter name.</summary>
    [Fact]
    public void ThrowsArgumentForInvalidCacheKeys()
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(default(string?), static key => _ = CacheKeyValidator.Validate(key, "entryKey"));
        Assert.Equal("entryKey", ex.ParamName);
        Assert.Equal("Cache key is required.", CacheKeyValidator.GetMessage(ServerKeyValidationError.Required));
        Assert.Contains("Cache key is required.", ex.Message, StringComparison.Ordinal);
    }
}
