using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
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

        // Reserve the primary port and the default management port at primary + 2.
        var ports = ListenPortPool.IntegrationTests.HoldPorts(3);
        try
        {
            var configure = Bind.To<(Uri Uri, TempDirectory Dir), Action<SquirixServerOptions>>(
                (ports[0].HttpUri, dir),
                static shared => options =>
                {
                    options.Uri = shared.Uri;
                    options.Resp = new RespOptions { Port = 0 };
                    options.UsePersistence(shared.Dir);
                });

            WriteEmptySegment(Path.Join(dir.ToString(), $"{FilePrefixes.Journal}000001{FileExtensions.Journal}"));

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(SquirixServer.StartAsync(configure, cancellationToken).AsTask(), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            ports[0].Dispose();
            await JournalSegmentLeaseWait.WaitForReleasedAsync(dir, cancellationToken);

            ports[2].Dispose();
            var server = await SquirixServer.StartAsync(configure, cancellationToken);
            await server.DisposeAsync();
        }
        finally
        {
            foreach (var port in ports)
                port.Return();
        }
    }

    private static void WriteEmptySegment(string path)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        File.WriteAllBytes(path, header);
    }
}
