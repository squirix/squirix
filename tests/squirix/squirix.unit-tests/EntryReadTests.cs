using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Testing;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.UnitTests;

/// <summary>Covers entry results and expiration metadata after a suspended remote response.</summary>
[Immutable]
public sealed class EntryReadTests
{
    /// <summary>A suspended entry read preserves misses, values and expiration rebasing on the client clock.</summary>
    /// <param name="scenario">Zero for a miss, one for a wire deadline, or two for a remaining lifetime.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    [Arguments(3)]
    public async Task EntryResultKeepsWireMetadata(int scenario)
    {
        var initial = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new FakeTimeProvider(initial);
        var wireDeadline = initial.UtcDateTime.AddDays(1);
        DateTime? transmittedDeadline = scenario == 3 ? null : wireDeadline;
        var response = new GetEntryAsyncResponse
        {
            Found = scenario != 0,
            Entry = scenario == 0
                ? null
                : new CacheEntryWire
                {
                    Value = new CacheValue { StringValue = "value" },
                    Expiration = TimeSpan.FromMinutes(2),
                    ExpiresUtc = transmittedDeadline,
                },
            Remaining = scenario >= 2 ? TimeSpan.FromSeconds(30) : null,
        };
        var release = new TaskCompletionSource<WireMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var client = new BinaryCacheClient(new Uri("https://localhost"), null, null, new EntryTransport(release.Task).InvokeAsync);
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(2));
        var expectations = IClientPool.Mock(MockBehavior.Strict);
        _ = expectations.ForNode(Arg.Any<string>()).Returns(new NodeClient(client, policy));
        var cache = new RemoteCache<string>("demo", new EndpointFailover(["node-a"], "node-a", TimeSpan.FromHours(1), clock), expectations.Object, RemoteClientSessionFactory.CreateSerializer());

        var operation = cache.GetEntryAsync("key-a", CancellationToken.None);
        var pending = !operation.IsCompleted;
        clock.Advance(TimeSpan.FromMinutes(1));
        _ = release.TrySetResult(response);
        var result = await operation.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(result.Found).IsEqualTo(scenario != 0);
        if (scenario == 0)
        {
            _ = await Assert.That(result.Entry).IsNull();
            return;
        }

        _ = await Assert.That(result.Value).IsEqualTo("value");
        _ = await Assert.That(result.Entry!.Expiration).IsEqualTo(TimeSpan.FromMinutes(2));
        _ = await Assert.That(result.ExpiresUtc).IsEqualTo(scenario >= 2 ? clock.GetUtcNow().UtcDateTime.AddSeconds(30) : wireDeadline);
    }

    private sealed class EntryTransport
    {
        private readonly Task<WireMessage> _response;

        internal EntryTransport(Task<WireMessage> response)
        {
            _response = response;
        }

        internal Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>> InvokeAsync =>
            (_, _, _, options) => _response.WaitAsync(options.CancellationToken);
    }
}
