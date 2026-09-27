using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.TestKit;
using TUnit.Core;

namespace Squirix.IntegrationTests;

/// <summary>Exported-client coverage for bootstrap connect against an unreachable endpoint.</summary>
[Immutable]
public sealed class BootstrapConnectTests : IntegrationTestBase
{
    /// <summary>Verifies caller cancellation ends a connect that is still retrying an unreachable bootstrap endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreachableConnectHonorsCancellation(CancellationToken cancellationToken)
    {
        // The bootstrap connect budget is far longer than this window, so only caller cancellation can end the call here.
        using var connectWindow = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        connectWindow.CancelAfter(TimeSpan.FromMilliseconds(200));

        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, ISquirixClient>(SquirixClient.ConnectAsync(new Uri("https://127.0.0.1:1"), connectWindow.Token));
    }
}
