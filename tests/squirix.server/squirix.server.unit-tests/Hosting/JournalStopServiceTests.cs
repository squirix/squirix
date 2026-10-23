using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>The hosted journal stop reports a journal that could not drain through the host stop, instead of hiding it in a disposal.</summary>
[Immutable]
public sealed class JournalStopServiceTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>A failed final flush of the journal fails the host stop, and the host disposal that follows stays quiet.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedJournalStopFailsHostStop(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, TimeSpan.FromSeconds(10), new EventRecordingLogger(), cancellationToken);
        var failure = new IOException("final fsync failed");
        journal.Writer.Flush.Arm();
        await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("a"), JournalEntryPayloadKit.EncodePut("v"), cancellationToken);
        await using var journalHost = new JournalCoordinatorHost(NullLoggerFactory.Instance, TimeProvider.System);
        journalHost.Attach(journal.Journal);
        using var host = new HostBuilder()
                         .ConfigureServices(services => services.AddHostedService(_ => new JournalStopService(journalHost.StopAsync)))
                         .Build();
        await host.StartAsync(cancellationToken);

        var stop = host.StopAsync(cancellationToken);
        await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
        journal.Writer.Flush.ReleaseWithFailure(failure);
        var reported = await NodeAsyncAssert.ThrowsAnyAsync<Exception>(stop.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken));

        _ = await Assert.That(reported is AggregateException aggregate ? aggregate.InnerException : reported).IsSameReferenceAs(failure);
        _ = await Assert.That(journal.Writer.DisposeCount).IsEqualTo(1);
    }

    /// <summary>A journal host that owns no journal has nothing to stop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HostWithoutJournalStopsQuietly(CancellationToken cancellationToken)
    {
        await using var journalHost = new JournalCoordinatorHost(NullLoggerFactory.Instance, TimeProvider.System);
        var service = new JournalStopService(journalHost.StopAsync);

        _ = await Assert.That(service.StoppedAsync(cancellationToken).IsCompletedSuccessfully).IsTrue();
    }
}
