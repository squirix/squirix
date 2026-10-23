using System;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies that a failed full server start releases the data directory so a later start on it succeeds.</summary>
public sealed class ServerStartRetryTests
{
    /// <summary>A start that fails to bind its listener after the journal opened releases the data directory, so a second start on it succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartRetriesOnSameDataDirectory(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-start-retry");
        using var blocker = new TcpListener(IPAddress.Loopback, 0);
        blocker.Start();
        var uri = new Uri($"https://localhost:{IPEndPoint.Parse(blocker.LocalEndpoint.ToString()!).Port}");

        WriteEmptySegment(Path.Join(dir.ToString(), $"{FilePrefixes.Journal}000001{FileExtensions.Journal}"));

        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(SquirixServer.StartAsync(Configure, cancellationToken).AsTask());
        blocker.Stop();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(dir, cancellationToken);

        var server = await SquirixServer.StartAsync(Configure, cancellationToken);
        await server.DisposeAsync();
        return;

        void Configure(SquirixServerOptions options)
        {
            options.Uri = uri;
            options.UsePersistence(dir);
        }
    }

    private static void WriteEmptySegment(string path)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        File.WriteAllBytes(path, header);
    }
}
