using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Checks the admission labels observed at the public binary endpoint.</summary>
public sealed class BinaryAdmissionOperationTests : NodeIntegrationTestBase
{
    /// <summary>Each wire operation reaches the gate under its established logical name.</summary>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    public async Task BinaryRequestsUseStableAdmissionLabels(CancellationToken cancellationToken)
    {
        const string node = "operation-labels";
        var options = new IntegrationStartOptions
        {
            ServicesConfigure = static services => services.AddSingleton<IBackpressureGate>(new RejectingGate()),
        };
        await using var cluster = await StartClusterAsync(node, options, cancellationToken);
        using var channel = CreateBinaryChannel(cluster[node].Uri);
        (BinaryOperation Op, WireMessage Request, string Label)[] cases =
        [
            (BinaryOperation.SetEntry, new SetEntryAsyncRequest(), "set"),
            (BinaryOperation.GetEntry, new GetEntryAsyncRequest(), "get_entry"),
            (BinaryOperation.GetValue, new GetValueAsyncRequest(), "get"),
            (BinaryOperation.Remove, new RemoveAsyncRequest(), "remove"),
            (BinaryOperation.TryAddEntry, new TryAddEntryAsyncRequest(), "try_add"),
            (BinaryOperation.Touch, new TouchAsyncRequest(), "touch"),
            (BinaryOperation.RemoveExpiration, new RemoveExpirationAsyncRequest(), "remove_expiration"),
            (BinaryOperation.Update, new UpdateAsyncRequest(), "update"),
            (BinaryOperation.GetExpiration, new GetExpirationAsyncRequest(), "get_entry"),
            (BinaryOperation.GetOrAdd, new GetOrAddAsyncRequest(), "get_or_add"),
            (BinaryOperation.SetDistributed, new SetDistributedAsyncRequest(), "set"),
            (BinaryOperation.GetDistributed, new GetDistributedAsyncRequest(), "get"),
            (BinaryOperation.RefreshDistributed, new RefreshDistributedAsyncRequest(), "touch"),
        ];
        foreach (var (operation, request, label) in cases)
        {
            var call = channel.ExchangeAsync(operation, request, SetAsyncResponse.Parser, null, cancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(call, TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(FrameStatus.ResourceExhausted);
            _ = await Assert.That(error.Message).Contains($"(cache/{label})");
        }
    }

    private sealed class RejectingGate : IBackpressureGate
    {
        public ValueTask<(Decision Decision, Lease Lease)> AcquireAsync(string transport, string operation, string clientId, CancellationToken cancellationToken) =>
            ValueTask.FromResult((Decision.Rejected($"{transport}/{operation}"), default(Lease)));
    }
}
