using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>In-memory model of the cache keys a durable mutation applied, with per-key apply signals.</summary>
[ThreadSafe]
internal sealed class AppliedKeys
{
    private readonly ConcurrentDictionary<string, TaskCompletionSource> _applied = new(StringComparer.Ordinal);

    /// <summary>Gets the applied keys in <see cref="StallableJournal.Describe" /> form.</summary>
    internal string Snapshot
    {
        get
        {
            var keys = new List<string>();
            foreach (var (key, signal) in _applied)
            {
                if (signal.Task.IsCompleted)
                    keys.Add(key);
            }

            return StallableJournal.Describe(keys);
        }
    }

    /// <summary>Runs one put through <paramref name="executor" />: journal append of <paramref name="key" />, then its memory apply.</summary>
    /// <param name="executor">Executor under test.</param>
    /// <param name="journal">Journal the executor writes to.</param>
    /// <param name="key">Default-namespace key to put.</param>
    /// <param name="cancellationToken">Caller cancellation token.</param>
    /// <returns>The mutation task.</returns>
    internal Task<int> PutAsync(DurableMutationExecutor executor, IJournalCoordinator journal, string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(executor);
        var cacheKey = CacheKey.Default(key);
        return executor.ExecuteAsync(
            cacheKey,
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, byte[] Payload, AppliedKeys Memory), int>(
                (journal, cacheKey, JournalEntryPayloadKit.EncodePut(key), this),
                static (s, ct) => s.Journal.AppendPutAsync(s.Key, s.Payload, ct),
                static (s, _) => s.Memory.ApplyAsync(s.Key)),
            cancellationToken).AsTask();
    }

    /// <summary>Runs one remove through <paramref name="executor" />: journal append of the removal, then its memory apply.</summary>
    /// <param name="executor">Executor under test.</param>
    /// <param name="journal">Journal the executor writes to.</param>
    /// <param name="key">Default-namespace key to remove.</param>
    /// <param name="cancellationToken">Caller cancellation token.</param>
    /// <returns>The mutation task.</returns>
    internal Task<int> RemoveAsync(DurableMutationExecutor executor, IJournalCoordinator journal, string key, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(executor);
        var cacheKey = CacheKey.Default(key);
        return executor.ExecuteAsync(
            cacheKey,
            static (_, _) => new ValueTask<DurableMutationCondition<int>>(DurableMutationCondition<int>.Apply()),
            new DurableMutationPipeline<(IJournalCoordinator Journal, CacheKey Key, AppliedKeys Memory), int>(
                (journal, cacheKey, this),
                static (s, ct) => s.Journal.AppendRemoveAsync(s.Key, ct),
                static (s, _) => s.Memory.UnapplyAsync(s.Key)),
            cancellationToken).AsTask();
    }

    private ValueTask<int> ApplyAsync(CacheKey key)
    {
        _ = Signal(key).TrySetResult();
        return ValueTask.FromResult(1);
    }

    private ValueTask<int> UnapplyAsync(CacheKey key)
    {
        _ = _applied.TryRemove(key.ToString(), out _);
        return ValueTask.FromResult(1);
    }

    private TaskCompletionSource Signal(CacheKey key) =>
        _applied.GetOrAdd(key.ToString(), static _ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously));
}
