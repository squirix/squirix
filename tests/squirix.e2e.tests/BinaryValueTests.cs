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
    /// <param name="size">The raw payload length, including an empty present value.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1024)]
    [Arguments(65536)]
    public async Task ClientReadsBinaryAfterRestart(int size, CancellationToken cancellationToken)
    {
        await using var cluster = await HostedCluster.StartSingleNodeAsync(nameof(ClientReadsBinaryAfterRestart), persistence: true, timeProvider: TimeProvider.System, cancellationToken: cancellationToken);
        var expected = new byte[size];
        for (var i = 0; i < expected.Length; i++)
            expected[i] = byte.CreateTruncating(i);
        await cluster.Cluster["nodeA"].SeedBinaryAsync("binary-client", "key", expected, cancellationToken);
        await cluster.RestartNodeAsync("nodeA", cancellationToken);
        var client = await cluster.GetCacheAsync<byte[]>("binary-client", cancellationToken: cancellationToken);
        var first = await client.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(first.Found).IsTrue();
        _ = await Assert.That(first.Value).IsNotNull();
        _ = await Assert.That(first.Value!.AsSpan().SequenceEqual(expected)).IsTrue();
        if (size > 0)
            first.Value[0] ^= 255;

        var second = await client.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(second.Found).IsTrue();
        _ = await Assert.That(second.Value!.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await client.RemoveAsync("key", cancellationToken);
        _ = await Assert.That((await client.GetValueAsync("key", cancellationToken)).Found).IsFalse();
    }
}
