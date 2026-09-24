using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Readiness while the journal pipeline is latched as failed: the node cannot commit until restart.</summary>
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

    private async Task<HttpStatusCode> GetReadyStatusCodeAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var response = await HttpClient.GetAsync(new Uri(uri, "/health/ready"), cancellationToken);
        return response.StatusCode;
    }
}
