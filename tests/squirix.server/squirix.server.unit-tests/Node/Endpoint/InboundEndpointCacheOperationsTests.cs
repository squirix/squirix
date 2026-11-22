using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Endpoint;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Endpoint;

/// <summary>Cache name routing coverage for the inbound endpoint cache operations.</summary>
[Immutable]
public sealed class InboundEndpointCacheOperationsTests
{
    /// <summary>Only the bounded set of cached facades remains alive with the singleton.</summary>
    [Test]
    public async Task FacadeRetentionIsBoundedTest()
    {
        var operations = CreateOperations();
        var references = CreateRequestFacades(operations);
#pragma warning disable S1215 // A forced collection verifies that the live singleton retains only the bounded set of facades.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
#pragma warning restore S1215

        var retained = 0;
        foreach (var reference in references)
        {
            if (reference.IsAlive)
                retained++;
        }

        GC.KeepAlive(operations);
        _ = await Assert.That(retained).IsEqualTo(InboundEndpointCacheLimits.MaxCachedFacades);
    }

    /// <summary>Repeated names reuse their facade, including after the retention limit is reached.</summary>
    [Test]
    public async Task CachedFacadeIsReusedAfterCapacityTest()
    {
        var operations = CreateOperations();
        var first = operations.ForCache("cache-0");
        _ = CreateRequestFacades(operations);

        _ = await Assert.That(operations.ForCache("cache-0")).IsSameReferenceAs(first);
        var overflow = operations.ForCache("overflow");
        _ = await Assert.That(operations.ForCache("overflow")).IsNotSameReferenceAs(overflow);
        _ = NodeExceptionAssert.For<ArgumentException>().Throws(operations, static ops => _ = ops.ForCache("bad name"));
    }

    /// <summary>Concurrent misses cannot exceed the facade retention limit.</summary>
    [Test]
    public async Task ConcurrentFacadeRetentionIsBoundedTest()
    {
        var operations = CreateOperations();
        var references = CreateConcurrentRequestFacades(operations);
#pragma warning disable S1215 // A forced collection verifies the retention bound after concurrent creation.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);
#pragma warning restore S1215

        var retained = 0;
        foreach (var reference in references)
        {
            if (reference.IsAlive)
                retained++;
        }

        GC.KeepAlive(operations);
        _ = await Assert.That(retained).IsEqualTo(InboundEndpointCacheLimits.MaxCachedFacades);
    }

    /// <summary>All allowed name characters are accepted.</summary>
    [Test]
    public async Task AllowedCharactersAreAccepted()
    {
        var operations = CreateOperations();

        _ = await Assert.That(operations.ForCache("Az09._-")).IsNotNull();
    }

    /// <summary>An invalid name throws on every call, including after valid names were used.</summary>
    [Test]
    public async Task InvalidNameKeepsThrowing()
    {
        var operations = CreateOperations();
        _ = operations.ForCache("cache-a");

        for (var attempt = 0; attempt < 2; attempt++)
        {
            var invalid = NodeExceptionAssert.For<ArgumentException>().Throws(operations, static ops => _ = ops.ForCache("bad name"));
            _ = await Assert.That(invalid.ParamName).IsEqualTo("cacheName");
            _ = await Assert.That(invalid.Message).Contains("Cache name contains invalid characters", StringComparison.Ordinal);
        }
    }

    /// <summary>Null, empty and whitespace-only names are rejected as required.</summary>
    [Test]
    public async Task MissingNameIsRejected()
    {
        var operations = CreateOperations();

        const string? missing = null;
        var nullName = NodeExceptionAssert.For<ArgumentException>().Throws(operations, missing, static (ops, name) => _ = ops.ForCache(name!));
        var empty = NodeExceptionAssert.For<ArgumentException>().Throws(operations, static ops => _ = ops.ForCache(string.Empty));
        var whitespace = NodeExceptionAssert.For<ArgumentException>().Throws(operations, static ops => _ = ops.ForCache("  "));

        _ = await Assert.That(nullName.Message).Contains("Cache name is required.", StringComparison.Ordinal);
        _ = await Assert.That(empty.Message).Contains("Cache name is required.", StringComparison.Ordinal);
        _ = await Assert.That(whitespace.Message).Contains("Cache name is required.", StringComparison.Ordinal);
    }

    /// <summary>Reserved and non-ASCII names are rejected.</summary>
    [Test]
    public async Task ReservedAndNonAsciiNamesAreRejected()
    {
        var operations = CreateOperations();
        var nonAscii = "caf" + char.ConvertFromUtf32(0xE9);

        var dot = NodeExceptionAssert.For<ArgumentException>().Throws(operations, static ops => _ = ops.ForCache(".."));
        var accented = NodeExceptionAssert.For<ArgumentException>().Throws(operations, nonAscii, static (ops, name) => _ = ops.ForCache(name));

        _ = await Assert.That(dot.Message).Contains("Cache name is reserved.", StringComparison.Ordinal);
        _ = await Assert.That(accented.Message).Contains("invalid characters", StringComparison.Ordinal);
    }

    private static InboundEndpointCacheOperations<string> CreateOperations() => new(new ILogicalNamespacedCacheCreateExpectations<string>().Instance());

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] CreateRequestFacades(InboundEndpointCacheOperations<string> operations)
    {
        var references = new WeakReference[InboundEndpointCacheLimits.MaxCachedFacades * 2];
        for (var i = 0; i < references.Length; i++)
            references[i] = new WeakReference(operations.ForCache($"cache-{i}"));

        return references;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static WeakReference[] CreateConcurrentRequestFacades(InboundEndpointCacheOperations<string> operations)
    {
        var references = new WeakReference[InboundEndpointCacheLimits.MaxCachedFacades * 4];
        _ = Parallel.For(0, references.Length, i => references[i] = new WeakReference(operations.ForCache($"parallel-{i}")));
        return references;
    }
}
