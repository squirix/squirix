using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The coordinator of a host whose writes commit through replica groups leaves deduplication to the group.</summary>
[Immutable]
public sealed class ReplicatedMutationCoordinatorTests : DisposableServerUnitTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>The fingerprint of a write is never computed: the group computes its own from the prepared mutation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NeverComputesFingerprint(CancellationToken cancellationToken)
    {
        var coordinator = new ReplicatedMutationCoordinator(null);

        var response = await coordinator.ExecuteAsync<int, TryAddAsyncResponse>(
            ValidOperationId,
            static _ => throw new InvalidOperationException("The fingerprint must not be computed."),
            0,
            static (_, _) => Task.FromResult(new TryAddAsyncResponse { Added = true }),
            cancellationToken);

        _ = await Assert.That(response.Added).IsTrue();
    }

    /// <summary>The handler runs on every call: no node-wide record answers a retry, the group does.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunsHandlerOnEveryCall(CancellationToken cancellationToken)
    {
        var coordinator = new ReplicatedMutationCoordinator(null);
        var runs = new int[1];

        for (var i = 0; i < 2; i++)
        {
            _ = await coordinator.ExecuteAsync(
                ValidOperationId,
                static _ => "unused",
                runs,
                static (counter, _) =>
                {
                    counter[0]++;
                    return Task.FromResult(new TryAddAsyncResponse { Added = true });
                },
                cancellationToken);
        }

        _ = await Assert.That(runs[0]).IsEqualTo(2);
    }

    /// <summary>A call without a valid operation identifier is refused before the handler runs.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesMissingOperationId(CancellationToken cancellationToken)
    {
        var coordinator = new ReplicatedMutationCoordinator(null);
        var runs = new int[1];

        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            coordinator.ExecuteAsync(
                string.Empty,
                static _ => "unused",
                runs,
                static (counter, _) =>
                {
                    counter[0]++;
                    return Task.FromResult(new TryAddAsyncResponse { Added = true });
                },
                cancellationToken));

        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(runs[0]).IsEqualTo(0);
    }

    /// <summary>The handler waits for the journal startup gate, which opens once local recovery replayed the journal into memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WaitsForJournalStartup(CancellationToken cancellationToken)
    {
        var startupGate = new AsyncManualResetEvent();
        var expectations = new IJournalCoordinatorCreateExpectations();
        _ = expectations.Setups.WaitForStartupAsync(Arg.Any<CancellationToken>()).Callback(startupGate.WaitAsync);
        _ = expectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        await using var journal = expectations.Instance();
        var coordinator = new ReplicatedMutationCoordinator(journal);
        var runs = new int[1];

        var call = coordinator.ExecuteAsync(
            ValidOperationId,
            static _ => "unused",
            runs,
            static (counter, _) =>
            {
                counter[0]++;
                return Task.FromResult(new TryAddAsyncResponse { Added = true });
            },
            cancellationToken);
        var runsBeforeStartup = runs[0];
        startupGate.Set();
        _ = await call.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(runsBeforeStartup).IsEqualTo(0);
        _ = await Assert.That(runs[0]).IsEqualTo(1);
    }
}
