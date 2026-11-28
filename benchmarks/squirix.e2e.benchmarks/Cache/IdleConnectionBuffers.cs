using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.Tracing;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2EBenchmarks.Support.Client;
using Squirix.E2EBenchmarks.Support.Cluster;

namespace Squirix.E2EBenchmarks.Cache;

/// <summary>Checks outstanding transport-sized pool rentals with both ends of idle TLS connections in one process.</summary>
internal sealed class IdleConnectionBuffers : EventListener
{
    private const int BufferSize = 16 * 1024;
    private int _outstanding;

    internal static async Task CheckAsync()
    {
        const int connectionCount = 32;
        using var timeout = new CancellationTokenSource(TimeSpan.FromMinutes(1));
        using var rentals = new IdleConnectionBuffers();
        var probe = ArrayPool<byte>.Shared.Rent(BufferSize);
        var observed = Volatile.Read(ref rentals._outstanding);
        ArrayPool<byte>.Shared.Return(probe);
        if (observed == 0 || Volatile.Read(ref rentals._outstanding) != 0)
            throw new InvalidOperationException("The runtime did not report the probe buffer rent and return.");

        var cluster = await E2EBenchmarkNodeCluster.StartAsync(cancellationToken: timeout.Token).ConfigureAwait(false);
        await using var ownedCluster = cluster.ConfigureAwait(false);
        var clients = new List<E2EBenchmarkClientLease>(connectionCount);

        try
        {
            var before = Volatile.Read(ref rentals._outstanding);
            for (var i = 0; i < connectionCount; i++)
                clients.Add(await cluster.OpenClientAsync(timeout.Token).ConfigureAwait(false));

            var retained = Volatile.Read(ref rentals._outstanding) - before;
            Console.WriteLine($"{connectionCount} idle connections retained {retained} additional {BufferSize}-byte pool rentals.");

            // Allow fixed background rentals, but catch even one large read buffer per connection on either side.
            if (retained > 4)
                throw new InvalidOperationException("Idle connections exceeded the pooled read-buffer budget.");
        }
        finally
        {
            foreach (var client in clients)
                await client.DisposeAsync().ConfigureAwait(false);
        }
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (string.Equals(eventSource.Name, "System.Buffers.ArrayPoolEventSource", StringComparison.Ordinal))
            EnableEvents(eventSource, EventLevel.Verbose);
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        // ArrayPoolEventSource: BufferRented = 1, BufferReturned = 3; payload[1] is the buffer length.
        if (eventData.EventId is 1 or 3 && eventData.Payload?[1] is BufferSize)
            Interlocked.Add(ref _outstanding, eventData.EventId == 1 ? 1 : -1);
    }
}
