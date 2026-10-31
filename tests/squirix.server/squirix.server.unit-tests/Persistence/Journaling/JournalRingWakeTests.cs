using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Verifies suppressed busy-consumer signals cannot lose work or control notifications.</summary>
public sealed class JournalRingWakeTests
{
    private static readonly TimeSpan Guard = TimeSpan.FromSeconds(10);

    /// <summary>An enqueue needs no kernel event signal when no consumer is preparing to park.</summary>
    [Test]
    public async Task BusyEnqueueDoesNotLatchSignal()
    {
        using var ring = new BoundedJournalRing(2, false);
        var item = JournalWorkItem.Shutdown();
        await ring.EnqueueAsync(item, CancellationToken.None);
        _ = await Assert.That(GetWorkSignal(ring).WaitOne(0)).IsFalse();
        _ = await Assert.That(ring.TryDequeue(out var found)).IsTrue();
        _ = await Assert.That(found).IsSameReferenceAs(item);
    }

    /// <summary>A control notification issued before registration still wakes a wait with no ring item.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ControlBeforeWaitRemainsLatched(CancellationToken cancellationToken)
    {
        using var ring = new BoundedJournalRing(2, false);
        ring.NotifyWorkAvailable();
        await using var waiter = ThreadRace.Start(
            ring,
            static r => r.WaitForWork(120_000, CancellationToken.None),
            static r => r.NotifyWorkAvailable(),
            cancellationToken);

        await waiter.Completion.WaitAsync(Guard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
    }

    /// <summary>The first enqueue wakes an already parked consumer with no earlier signal to rescue it.</summary>
    /// <param name="useHighResolutionTimer">Whether finite waits may use the high-resolution timer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EnqueueWakesParkedConsumer(bool useHighResolutionTimer, CancellationToken cancellationToken)
    {
        using var ring = new BoundedJournalRing(2, useHighResolutionTimer);
        var thread = new StrongBox<Thread?>();
        using var parked = new ManualResetEventSlim(false);
        await using var waiter = ThreadRace.Start(
            (Ring: ring, Thread: thread, Parked: parked),
            static s =>
            {
                Volatile.Write(ref s.Thread.Value, Thread.CurrentThread);
                s.Parked.Set();
                s.Ring.WaitForWork(120_000, CancellationToken.None);
            },
            static s => s.Ring.NotifyWorkAvailable(),
            cancellationToken);

        _ = await Assert.That(parked.Wait(Guard, cancellationToken)).IsTrue();
        await Volatile.Read(ref thread.Value).WaitUntilAsync(static t => IsBlocked(t), Guard, TimeProvider.System, cancellationToken);
        var item = JournalWorkItem.Shutdown();
        await ring.EnqueueAsync(item, cancellationToken);
        await waiter.Completion.WaitAsync(Guard, TimeProvider.System, cancellationToken);
        _ = await Assert.That(ring.TryDequeue(out var found)).IsTrue();
        _ = await Assert.That(found).IsSameReferenceAs(item);
    }

    /// <summary>Repeated publication races with wait registration from an empty ring without losing a wakeup or item.</summary>
    /// <param name="useHighResolutionTimer">Whether finite waits may use the high-resolution timer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task EmptyRingWaitRacingPublishConservesWork(bool useHighResolutionTimer, CancellationToken cancellationToken)
    {
        using var ring = new BoundedJournalRing(1, useHighResolutionTimer);
        using var phases = new Barrier(2);
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(Guard);
        var run = new WakeRun(ring, phases, guard.Token);
        var consumer = Task.Factory.StartNew(run.Consume, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        var producer = Task.Factory.StartNew(run.ProduceAsync, CancellationToken.None, TaskCreationOptions.DenyChildAttach, TaskScheduler.Default).Unwrap();
        try
        {
            await Task.WhenAll(consumer, producer).WaitAsync(Guard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await guard.CancelAsync();
            ring.NotifyWorkAvailable();
            await Task.WhenAll(consumer, producer).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        _ = await Assert.That(run.Invalid).IsFalse();
        _ = await Assert.That(run.Completed).IsEqualTo(256);
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
    }

    /// <summary>Several producers racing an idle consumer preserve every distinct item and release every ring slot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MultipleProducersConserveWork(CancellationToken cancellationToken)
    {
        using var ring = new BoundedJournalRing(4, false);
        using var guard = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        guard.CancelAfter(Guard);
        var run = new ProducerRun(ring, guard.Token);
        var tasks = new Task[9];
        tasks[0] = Task.Factory.StartNew(run.Consume, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
        for (var i = 0; i < 8; i++)
            tasks[i + 1] = run.ProduceAsync(i);
        run.Start();
        try
        {
            await Task.WhenAll(tasks).WaitAsync(Guard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await guard.CancelAsync();
            ring.NotifyWorkAvailable();
            await Task.WhenAll(tasks).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }

        _ = await Assert.That(run.AllSeenOnce).IsTrue();
        _ = await Assert.That(ring.TryDequeue(out _)).IsFalse();
        for (var i = 0; i < 4; i++)
            await ring.EnqueueAsync(JournalWorkItem.Shutdown(), cancellationToken);
        for (var i = 0; i < 4; i++)
            _ = await Assert.That(ring.TryDequeue(out _)).IsTrue();
    }

    [UnsafeAccessor(UnsafeAccessorKind.Field, Name = "_workSignal")]
    private static extern ref AutoResetEvent GetWorkSignal(BoundedJournalRing ring);

    private static bool IsBlocked(Thread? thread) => thread != null && (thread.ThreadState & System.Threading.ThreadState.WaitSleepJoin) == System.Threading.ThreadState.WaitSleepJoin;

    private sealed class ProducerRun
    {
        private readonly BoundedJournalRing _ring;
        private readonly int[] _seen = new int[1024];
        private readonly TaskCompletionSource _start = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly CancellationToken _token;
        private bool _invalid;

        internal ProducerRun(BoundedJournalRing ring, CancellationToken token)
        {
            _ring = ring;
            _token = token;
        }

        internal bool AllSeenOnce
        {
            get
            {
                for (var i = 0; i < _seen.Length; i++)
                {
                    if (_seen[i] != 1)
                        return false;
                }

                return !_invalid;
            }
        }

        internal void Consume()
        {
            for (var count = 0; count < _seen.Length;)
            {
                _token.ThrowIfCancellationRequested();
                if (_ring.TryDequeue(out var item))
                {
                    var id = BitConverter.ToInt32(item.FrameBytes ?? [], 0);
                    if (id >= 0 && id < _seen.Length)
                        _seen[id]++;
                    else
                        _invalid = true;
                    count++;
                }
                else
                {
                    _ring.WaitForWork(60_000, CancellationToken.None);
                }
            }
        }

        internal async Task ProduceAsync(int producer)
        {
            await _start.Task.ConfigureAwait(false);
            for (var i = 0; i < 128; i++)
            {
                var bytes = BitConverter.GetBytes((producer * 128) + i);
                await _ring.EnqueueAsync(JournalWorkItem.Append(bytes, bytes.Length), _token).ConfigureAwait(false);
                if ((i & 7) == 0)
                    await Task.Yield();
            }
        }

        internal void Start() => _ = _start.TrySetResult();
    }

    private sealed class WakeRun
    {
        private readonly JournalWorkItem[] _items = new JournalWorkItem[256];
        private readonly Barrier _phases;
        private readonly BoundedJournalRing _ring;
        private readonly CancellationToken _token;
        private int _completed;
        private int _invalid;

        internal WakeRun(BoundedJournalRing ring, Barrier phases, CancellationToken token)
        {
            _ring = ring;
            _phases = phases;
            _token = token;
            for (var i = 0; i < _items.Length; i++)
                _items[i] = JournalWorkItem.Shutdown();
        }

        internal int Completed => Volatile.Read(ref _completed);

        internal bool Invalid => Volatile.Read(ref _invalid) != 0;

        internal void Consume()
        {
            for (var i = 0; i < _items.Length; i++)
            {
                _phases.SignalAndWait(_token);
                JournalWorkItem? item;
                while (!_ring.TryDequeue(out item))
                {
                    _token.ThrowIfCancellationRequested();
                    _ring.WaitForWork(60_000, CancellationToken.None);
                }

                if (!ReferenceEquals(item, _items[i]))
                    _ = Interlocked.Exchange(ref _invalid, 1);
                _ = Interlocked.Increment(ref _completed);
                _phases.SignalAndWait(_token);
            }
        }

        internal async Task ProduceAsync()
        {
            for (var i = 0; i < _items.Length; i++)
            {
                _phases.SignalAndWait(_token);
                await _ring.EnqueueAsync(_items[i], _token).ConfigureAwait(false);
                _phases.SignalAndWait(_token);
            }
        }
    }
}
