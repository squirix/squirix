using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The hosted journal stop reports a journal that could not drain through the host stop, instead of hiding it in a disposal.</summary>
[Immutable]
public sealed class JournalStopServiceTests : IsolatedStorageTestBase
{
    /// <summary>A journal host that owns no journal has nothing to stop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostWithoutJournalStopsQuietly(CancellationToken cancellationToken)
    {
        await using var journalHost = new JournalCoordinatorHost();
        var service = new JournalStopService(journalHost);

        _ = await Assert.That(service.StoppedAsync(cancellationToken).IsCompletedSuccessfully).IsTrue();
    }
}
