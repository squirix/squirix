using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for <see cref="CacheConflictException" />.</summary>
[Immutable]
public sealed class CacheConflictExceptionTests
{
    /// <summary>Ensures the conflicting key is exposed on the exception.</summary>
    [Test]
    public async Task KeyExposesConflictingCacheKey()
    {
        var ex = new CacheConflictException("orders:42");

        _ = await Assert.That(ex.Key).IsEqualTo("orders:42");
        _ = await Assert.That(ex.Message).Contains("orders:42", StringComparison.Ordinal);
    }
}
