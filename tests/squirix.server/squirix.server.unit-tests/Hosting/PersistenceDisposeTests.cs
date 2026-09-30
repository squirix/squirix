using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>
/// Host disposal over a journal whose dispose fails (a leaked journal I/O thread): the journal host logs the failure and the container keeps
/// disposing the persistence runtime instead of aborting.
/// </summary>
[Immutable]
public sealed class PersistenceDisposeTests : IsolatedStorageTestBase
{
    private const int JournalDisposeFailedEventId = 3016;

    /// <summary>A throwing journal dispose neither escapes the container nor skips the manifest ledger.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LedgerDisposedWhenJournalDisposeThrows(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-persistence-dispose");
        var provider = await BuildProviderAsync(meter, null, cancellationToken);
        var ledger = provider.GetRequiredService<Ledger>();
        _ = AttachThrowingJournal(provider);

        await provider.DisposeAsync();

        _ = await NodeAsyncAssert.ThrowsAsync<ObjectDisposedException>(ledger.ReadCurrentOrDefaultAsync(cancellationToken));
    }

    /// <summary>A throwing journal dispose is reported at error level with the journal failure as its cause.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournalDisposeFailureLoggedAsError(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-persistence-dispose-log");
        var log = new EventRecordingLogger();
        var provider = await BuildProviderAsync(meter, log, cancellationToken);
        var failure = AttachThrowingJournal(provider);

        await provider.DisposeAsync();

        var entry = log.Find(JournalDisposeFailedEventId);

        _ = await Assert.That(entry?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(entry?.Cause).IsSameReferenceAs(failure);
    }

    /// <summary>Resolving the journal before storage is opened fails with an explicit error instead of a null journal.</summary>
    [Test]
    public async Task ResolvingJournalBeforeOpenThrows()
    {
        using var meter = new Meter("test-persistence-unopened");
        await using var provider = BuildUnopenedProvider(meter, null);

        var thrown = NodeExceptionAssert.For<InvalidOperationException>().Throws(provider, static services => _ = services.GetRequiredService<IJournalCoordinator>());

        _ = await Assert.That(thrown.Message).Contains("MapSquirixServerAsync", StringComparison.Ordinal);
    }

    /// <summary>Opening storage a second time is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpeningStorageTwiceThrows(CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-persistence-open-twice");
        await using var provider = await BuildProviderAsync(meter, null, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(PersistenceServiceRegistration.OpenPersistenceAsync(provider, cancellationToken));
    }

    /// <summary>Hands the journal host a journal whose dispose fails over the real journal it owns.</summary>
    /// <param name="provider">The persistence service provider.</param>
    /// <returns>The failure the journal dispose throws.</returns>
    private static TimeoutException AttachThrowingJournal(IServiceProvider provider)
    {
        var host = provider.GetRequiredService<JournalCoordinatorHost>();
        var failure = new TimeoutException("journal I/O thread is still alive after shutdown; writer, ring, and gates are leaked.");
        AttachThrowingJournal(host, host.Coordinator, failure);
        return failure;
    }

    /// <summary>Hands <paramref name="host" /> a journal that disposes <paramref name="journal" /> and then fails, as a journal whose I/O thread leaked on shutdown does.</summary>
    /// <param name="host">The journal host.</param>
    /// <param name="journal">The real journal, taken before the host hands out the throwing one.</param>
    /// <param name="failure">The failure the dispose throws.</param>
    private static void AttachThrowingJournal(JournalCoordinatorHost host, IJournalCoordinator journal, TimeoutException failure)
    {
        var expectations = new IJournalCoordinatorCreateExpectations();
        _ = expectations.Setups.DisposeAsync().Callback(async () =>
        {
            await journal.DisposeAsync();
            throw failure;
        });
        host.Attach(expectations.Instance());
    }

    private async Task<ServiceProvider> BuildProviderAsync(Meter meter, EventRecordingLogger? log, CancellationToken cancellationToken)
    {
        var provider = BuildUnopenedProvider(meter, log);
        await PersistenceServiceRegistration.OpenPersistenceAsync(provider, cancellationToken);
        return provider;
    }

    private ServiceProvider BuildUnopenedProvider(Meter meter, EventRecordingLogger? log)
    {
        var services = new ServiceCollection();
        _ = services.AddLogging(builder =>
        {
            if (log != null)
                _ = builder.AddProvider(new RecordingLoggerProvider(log));
        });

        _ = services.AddPersistenceServices(new PersistenceOptions { DataDir = Dir }, meter, false);
        return services.BuildServiceProvider();
    }
}
