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
