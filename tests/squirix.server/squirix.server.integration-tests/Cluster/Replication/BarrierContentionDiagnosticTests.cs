using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>TEMP DIAGNOSTIC for issue #609: records journal barrier spans around an RF=2 commit (revert before merge).</summary>
public sealed class BarrierContentionDiagnosticTests : NodeIntegrationTestBase
{
    /// <summary>Records per-node journal spans for one RF=2 write; the dump identifies the barrier holder on failure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Repeat(100)]
    public async Task RfTwoCommitRecordsBarrierSpans(CancellationToken cancellationToken)
    {
        var uriA = GetNextHttpUri();
        var uriB = GetNextHttpUri();
        var peers = BuildClusterPeers([("node-a", uriA), ("node-b", uriB)]);
        var recorderA = new JournalBarrierRecorder();
        var recorderB = new JournalBarrierRecorder();
        var optionsA = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "barrier-spans", ServicesConfigure = b => b.AddSingleton<IJournalOperationTracer>(recorderA) };
        var optionsB = new NodeStartOptions { ReplicaCount = 2, UsePersistence = true, ExtraScope = "barrier-spans", ServicesConfigure = b => b.AddSingleton<IJournalOperationTracer>(recorderB) };

        await using var nodeA = await StartNodeAsync(uriA, peers, optionsA, cancellationToken);
        await using var nodeB = await StartNodeAsync(uriB, peers, optionsB, cancellationToken);

        try
        {
            var cache = nodeA.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("leader-read");
            var key = FindOwnedKey(nodeA, "leader-read", "node-a");
            await cache.SetEntryAsync(Guid.NewGuid().ToString("N"), "leader-read", key, new NodeCacheEntry<object?> { Value = "v" }, cancellationToken);

            // ReSharper disable once DisposeOnUsingVariable — intentional peer loss, mirrors the flaky test.
            await nodeB.DisposeAsync();

            var read = await cache.GetValueAsync("leader-read", key, cancellationToken);
            _ = await Assert.That(read.Found).IsTrue();
        }
        finally
        {
            recorderA.Dump("node-a");
            recorderB.Dump("node-b");
        }
    }

    /// <summary>TEMP DIAGNOSTIC: finds a key owned by a node (revert before merge).</summary>
    /// <param name="host">Test node host.</param>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="owner">Expected owner node identifier.</param>
    /// <exception cref="InvalidOperationException">No owned key was found.</exception>
    private static string FindOwnedKey(TestNodeHost host, string cacheName, string owner)
    {
        var locator = host.Services.GetRequiredService<INodeLocator>();
        for (var i = 0; i < 10_000; i++)
        {
            var candidate = $"barrier-spans-{i}";
            if (string.Equals(locator.GetOwner(cacheName, candidate), owner, StringComparison.Ordinal))
                return candidate;
        }

        throw new InvalidOperationException($"No key owned by '{owner}' was found.");
    }

    /// <summary>TEMP DIAGNOSTIC: records journal operation spans (revert before merge).</summary>
    private sealed class JournalBarrierRecorder : IJournalOperationTracer
    {
        private readonly ConcurrentQueue<JournalBarrierSpan> _spans = new();

        IJournalOperationTraceScope? IJournalOperationTracer.Begin(JournalOperationKind kind, in JournalOperationTraceContext? context) =>
            new Scope(this, kind, Stopwatch.GetTimestamp());

        internal void Dump(string title)
        {
            Console.WriteLine($"[probe] {title}:");
            foreach (var span in _spans)
                Console.WriteLine($"[probe] {span.Kind} {span.ElapsedMs}ms");
        }

        [StructLayout(LayoutKind.Auto)]
        private readonly struct JournalBarrierSpan
        {
            internal JournalBarrierSpan(JournalOperationKind kind, long elapsedMs)
            {
                Kind = kind;
                ElapsedMs = elapsedMs;
            }

            internal JournalOperationKind Kind { get; }

            internal long ElapsedMs { get; }
        }

        private sealed class Scope : IJournalOperationTraceScope
        {
            private readonly JournalOperationKind _kind;
            private readonly JournalBarrierRecorder _owner;
            private readonly long _start;

            internal Scope(JournalBarrierRecorder owner, JournalOperationKind kind, long start)
            {
                _owner = owner;
                _kind = kind;
                _start = start;
            }

            void IDisposable.Dispose()
            {
                var elapsedMs = (Stopwatch.GetTimestamp() - _start) * 1000 / Stopwatch.Frequency;
                _owner._spans.Enqueue(new JournalBarrierSpan(_kind, elapsedMs));
            }
        }
    }
}
