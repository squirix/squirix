using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Core;

/// <summary>Public cache-name parsing preserves validated names and rejects invalid input without a fallback.</summary>
[Immutable]
public sealed class ServerCacheNameTests
{
    /// <summary>Both parsing paths preserve the exact validated string.</summary>
    /// <param name="name">A valid public cache name.</param>
    [Test]
    [Arguments("default")]
    [Arguments("Az09._-")]
    [Arguments("Orders-v2")]
    public async Task ParsingPreservesValidatedName(string name)
    {
        var accepted = ServerCacheName.TryParsePublic(name, out var canonical);

        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(canonical).IsSameReferenceAs(name);
        _ = await Assert.That(ServerCacheName.ParsePublic(name).Canonical).IsSameReferenceAs(name);
    }

    /// <summary>Invalid input yields false and null, and the throwing path keeps its parameter name.</summary>
    /// <param name="name">An invalid public cache name.</param>
    [Test]
    [Arguments(null)]
    [Arguments("")]
    [Arguments("  ")]
    [Arguments(".")]
    [Arguments("..")]
    [Arguments("bad name")]
    [Arguments("bad/name")]
    [Arguments("café")]
    public async Task ParsingRejectsInvalidName(string? name)
    {
        var accepted = ServerCacheName.TryParsePublic(name, out var canonical);
        var failure = NodeExceptionAssert.For<ArgumentException>().Throws(name, static value => _ = ServerCacheName.ParsePublic(value, nameof(name)));

        _ = await Assert.That(accepted).IsFalse();
        _ = await Assert.That(canonical).IsNull();
        _ = await Assert.That(failure.ParamName).IsEqualTo(nameof(name));
    }

    /// <summary>The maximum length is accepted unchanged and the next length is rejected.</summary>
    [Test]
    public async Task ParsingEnforcesLengthLimit()
    {
        var limit = new string('a', 128);
        var overLimit = new string('a', 129);

        _ = await Assert.That(ServerCacheName.TryParsePublic(limit, out var canonical)).IsTrue();
        _ = await Assert.That(canonical).IsSameReferenceAs(limit);
        _ = await Assert.That(ServerCacheName.ParsePublic(limit).Canonical).IsSameReferenceAs(limit);
        _ = await Assert.That(ServerCacheName.TryParsePublic(overLimit, out var rejected)).IsFalse();
        _ = await Assert.That(rejected).IsNull();
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(overLimit, static value => _ = ServerCacheName.ParsePublic(value));
    }
}
