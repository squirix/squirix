using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Readiness while the journal pipeline is latched as failed (the node cannot commit until restart) or its I/O is stalled.</summary>
public sealed class JournalLatchReadinessTests : NodeIntegrationTestBase
{
    private const string JournalMaintenanceCheck = "journal_maintenance";

    /// <summary>An ephemeral node has no journal, so it registers no journal readiness check and stays ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EphemeralNodeStaysReady(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node_latch_ephemeral", new IntegrationStartOptions(), cancellationToken);
        var node = cluster["node_latch_ephemeral"];

        var registrations = node.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations;
        var hasJournalCheck = false;
        foreach (var registration in registrations)
            hasJournalCheck |= string.Equals(registration.Name, JournalMaintenanceCheck, StringComparison.Ordinal);

        _ = await Assert.That(hasJournalCheck).IsFalse();
        _ = await Assert.That(await GetReadyStatusCodeAsync(node.Uri, cancellationToken)).IsEqualTo(HttpStatusCode.OK);
    }

    /// <summary>A persistent node turns <c language="csharp">/health/ready</c> unhealthy once its journal pipeline latches a failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LatchedJournalFailsReadiness(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node_latch_journal", new IntegrationStartOptions { UsePersistence = true }, cancellationToken);
        var node = cluster["node_latch_journal"];
        var readyBefore = await GetReadyStatusCodeAsync(node.Uri, cancellationToken);

        node.GetRequiredService<IJournalCoordinator>().FailJournalPipeline(new IOException("simulated fsync failure"));

        _ = await Assert.That(readyBefore).IsEqualTo(HttpStatusCode.OK);
        _ = await Assert.That(await GetReadyStatusCodeAsync(node.Uri, cancellationToken)).IsEqualTo(HttpStatusCode.ServiceUnavailable);
    }

    /// <summary>
    /// A persistent node reports <c language="csharp">/health/ready</c> degraded while a journal segment I/O call is in progress past the
    /// threshold and ready again once it returns. The host keeps the default ASP.NET Core status mapping, so degraded answers HTTP 200
    /// like healthy and only the body (<c language="text">Degraded</c>) tells them apart.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The host journal does not expose its stall probe.</exception>
    [Test]
    public async Task StalledJournalIoDegradesReady(CancellationToken cancellationToken)
    {
        // A one-tick threshold: any operation still in progress when the probe is read counts as stalled.
        var persistence = new PersistenceOptions { JournalStallDegradedThreshold = TimeSpan.FromTicks(1) };
        await using var cluster = await StartClusterAsync("node_stall_journal", new IntegrationStartOptions { PersistenceOptions = persistence }, cancellationToken);
        var node = cluster["node_stall_journal"];
        if (node.GetRequiredService<JournalCoordinatorHost>().Coordinator is not IJournalCoordinatorState journal)
            throw new InvalidOperationException("the host journal does not expose its stall probe.");

        var before = await GetReadyAsync(node.Uri, cancellationToken);

        // The node is idle, so its journal thread performs no segment I/O and the test is the only writer of the probe.
        journal.StallProbe.IoStarted(nameof(IJournalSegmentWriter.FlushToDisk));
        (HttpStatusCode Status, string Body) stalled;
        try
        {
            stalled = await GetReadyAsync(node.Uri, cancellationToken);
        }
        finally
        {
            journal.StallProbe.IoFinished();
        }

        var after = await GetReadyAsync(node.Uri, cancellationToken);

        _ = await Assert.That(before).IsEqualTo((HttpStatusCode.OK, nameof(HealthStatus.Healthy)));
        _ = await Assert.That(stalled).IsEqualTo((HttpStatusCode.OK, nameof(HealthStatus.Degraded)));
        _ = await Assert.That(after).IsEqualTo((HttpStatusCode.OK, nameof(HealthStatus.Healthy)));
    }

    private async Task<(HttpStatusCode Status, string Body)> GetReadyAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);
        return (response.StatusCode, await response.Content.ReadAsStringAsync(cancellationToken));
    }

    private async Task<HttpStatusCode> GetReadyStatusCodeAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);
        return response.StatusCode;
    }
}
