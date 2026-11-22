using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Checks that the exported client reads server-owned binary values through real transport and recovery.</summary>
[Immutable]
public sealed class BinaryValueTests
{
    /// <summary>The SDK sends and reads a typed value that needs extended frames.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TypedSdkUsesExtendedFrames(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(TypedSdkUsesExtendedFrames), timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var cache = await cluster.GetCacheAsync<string>("extended-frames", cancellationToken: cancellationToken);
        var value = new string('x', 9 * 1024 * 1024) + "end";

        await cache.SetAsync("large", value, cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("large", cancellationToken);

        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(string.Equals(value, result.Value, StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Binary reads survive restart, return owned arrays and observe removal on the server.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientReadsBinaryAfterRestart(CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(ClientReadsBinaryAfterRestart), persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        (string Key, byte[] Expected)[] entries = [("empty", []), ("small", new byte[1024]), ("large", new byte[65536])];
        foreach (var (key, expected) in entries)
        {
            for (var index = 0; index < expected.Length; index++)
                expected[index] = byte.CreateTruncating(index);

            await cluster.Cluster["nodeA"].SeedBinaryAsync("binary-client", key, expected, cancellationToken);
        }

        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var client = await cluster.GetCacheAsync<byte[]>("binary-client", cancellationToken: cancellationToken);
        foreach (var (key, expected) in entries)
        {
            var first = await client.GetValueAsync(key, cancellationToken);
            _ = await Assert.That(first.Found).IsTrue();
            _ = await Assert.That(first.Value).IsNotNull();
            _ = await Assert.That(first.Value!.AsSpan().SequenceEqual(expected)).IsTrue();
            if (expected.Length > 0)
                first.Value[0] ^= 255;

            var second = await client.GetValueAsync(key, cancellationToken);
            _ = await Assert.That(second.Found).IsTrue();
            _ = await Assert.That(second.Value!.AsSpan().SequenceEqual(expected)).IsTrue();
            _ = await client.RemoveAsync(key, cancellationToken);
            _ = await Assert.That((await client.GetValueAsync(key, cancellationToken)).Found).IsFalse();
        }
    }
}
