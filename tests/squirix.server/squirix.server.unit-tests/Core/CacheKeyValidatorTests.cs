using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Covers cache key validation and display formatting.</summary>
[Immutable]
public sealed class CacheKeyValidatorTests : ServerUnitTestBase
{
    /// <summary>CacheKey formats default and namespaced keys.</summary>
    [Test]
    public async Task CacheKeyToStringFormatsNamespaceAndKey()
    {
        _ = await Assert.That(new CacheKey(string.Empty, "plain").ToString()).IsEqualTo("plain");
        _ = await Assert.That(new CacheKey("ns", "plain").ToString()).IsEqualTo("ns:plain");
        _ = await Assert.That(CacheKey.Default("k").ToString()).IsEqualTo(ServerCacheNames.DefaultNamespace + ":k");
    }

    /// <summary>GetMessage rejects unknown enum values.</summary>
    [Test]
    public void GetMessageRejectsUnknownError()
    {
        var raw = 42;
        var error = Unsafe.As<int, ServerKeyValidationError>(ref raw);
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(error, static value => _ = CacheKeyValidator.GetMessage(value));
    }

    /// <summary>Validate throws ArgumentException with the caller parameter name.</summary>
    [Test]
    public async Task ThrowsArgumentForInvalidCacheKeys()
    {
        var ex = NodeExceptionAssert.For<ArgumentException>().Throws(default(string?), static key => _ = CacheKeyValidator.Validate(key, "entryKey"));
        _ = await Assert.That(ex.ParamName).IsEqualTo("entryKey");
        _ = await Assert.That(CacheKeyValidator.GetMessage(ServerKeyValidationError.Required)).IsEqualTo("Cache key is required.");
        _ = await Assert.That(ex.Message).Contains("Cache key is required.", StringComparison.Ordinal);
    }

    /// <summary>Accepts ordinary keys and returns stable required/control/too-long diagnostics.</summary>
    [Test]
    public async Task ValidateCoversSuccessAndFailurePaths()
    {
        _ = await Assert.That(CacheKeyValidator.TryValidate("ok", out var ok)).IsTrue();
        _ = await Assert.That(ok).IsEqualTo(default);
        _ = await Assert.That(CacheKeyValidator.Validate("ok", "key")).IsEqualTo("ok");

        _ = await Assert.That(CacheKeyValidator.TryValidate(null, out var required)).IsFalse();
        _ = await Assert.That(required).IsEqualTo(ServerKeyValidationError.Required);
        _ = await Assert.That(CacheKeyValidator.GetMessage(required)).IsEqualTo("Cache key is required.");

        _ = await Assert.That(CacheKeyValidator.TryValidate("   ", out var whitespace)).IsFalse();
        _ = await Assert.That(whitespace).IsEqualTo(ServerKeyValidationError.Required);

        _ = await Assert.That(CacheKeyValidator.TryValidate("a\tb", out var control)).IsFalse();
        _ = await Assert.That(control).IsEqualTo(ServerKeyValidationError.ControlCharacters);
        _ = await Assert.That(CacheKeyValidator.GetMessage(control)).IsEqualTo("Cache key contains control characters.");

        var tooLong = new string('k', 1025);
        _ = await Assert.That(CacheKeyValidator.TryValidate(tooLong, out var length)).IsFalse();
        _ = await Assert.That(length).IsEqualTo(ServerKeyValidationError.TooLong);
        _ = await Assert.That(CacheKeyValidator.GetMessage(length)).IsEqualTo("Cache key exceeds the maximum length of 1024 characters.");

        var max = new string('k', 1024);
        _ = await Assert.That(CacheKeyValidator.TryValidate(max, out _)).IsTrue();
    }
}
