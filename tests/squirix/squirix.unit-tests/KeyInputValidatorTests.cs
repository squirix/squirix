using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Squirix.Attributes;
using Squirix.Core;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Tests for centralized cache key validation.</summary>
[UsedImplicitly]
[Immutable]
public sealed class KeyInputValidatorTests : UnitTestBase
{
    /// <summary>Gets invalid keys and canonical messages.</summary>
    public static IEnumerable<(string? Key, string ExpectedMessage)> InvalidKeys()
    {
        yield return (null, "Cache key is required.");
        yield return (string.Empty, "Cache key is required.");
        yield return ("   ", "Cache key is required.");
        yield return (new string('a', KeyInputValidator.MaxLength + 1), $"Cache key exceeds the maximum length of {KeyInputValidator.MaxLength} characters.");
        yield return ("a\u0001b", "Cache key contains control characters.");
        yield return ("\t", "Cache key is required.");
    }

    /// <summary>Verifies max length key is accepted.</summary>
    [Test]
    public void ValidateAcceptsMaxLengthKey()
    {
        var key = new string('x', KeyInputValidator.MaxLength);
        KeyInputValidator.Validate(key, nameof(key));
    }

    /// <summary>Verifies common separators and Unicode are accepted.</summary>
    /// <param name="key">The key to validate.</param>
    [Test]
    [Arguments("user:123")]
    [Arguments("tenant/acme/cart/42")]
    [Arguments("email:test@example.com")]
    [Arguments("order#123")]
    [Arguments("a/b:c.d_e-f@g#h?i=")]
    [Arguments("emoji:🔑")]
    public void ValidateAcceptsSeparatorsAndUnicode(string key) => KeyInputValidator.Validate(key, nameof(key));

    /// <summary>Verifies invalid keys fail with deterministic messages.</summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="expectedMessage">The expected canonical message.</param>
    [Test]
    [MethodDataSource(nameof(InvalidKeys))]
    public async Task ValidateRejectsInvalidKeys(string? key, string expectedMessage)
    {
        var ex = ExceptionAssert.For<ArgumentException>().Throws(key, static value => KeyInputValidator.Validate(value, nameof(value)));

        _ = await Assert.That(ex.Message).StartsWith(expectedMessage, StringComparison.Ordinal);
    }
}
