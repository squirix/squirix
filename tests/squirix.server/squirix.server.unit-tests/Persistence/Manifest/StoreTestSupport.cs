using System;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;

namespace Squirix.Server.UnitTests.Persistence.Manifest;

/// <summary>Helpers for manifest store tests.</summary>
internal static class StoreTestSupport
{
    internal const string JournalSegment000001 = $"{FilePrefixes.Journal}000001{FileExtensions.Journal}";
    internal const string JournalSegment000002 = $"{FilePrefixes.Journal}000002{FileExtensions.Journal}";
    internal const string JournalSegment000003 = $"{FilePrefixes.Journal}000003{FileExtensions.Journal}";
    internal const string Manifest000001 = $"{FilePrefixes.Manifest}000001{FileExtensions.Manifest}";
    internal const string Manifest000003 = $"{FilePrefixes.Manifest}000003{FileExtensions.Manifest}";
    internal const string Snapshot000001 = $"{FilePrefixes.Snapshot}000001{FileExtensions.Snapshot}";
    internal const string Snapshot000002 = $"{FilePrefixes.Snapshot}000002{FileExtensions.Snapshot}";

    internal static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
    };

    internal static string ManifestDataFileName(int index) => $"{FilePrefixes.Manifest}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Manifest}";

    internal static async Task<int> ReadCurrentManifestIndexAsync(string dataDir, CancellationToken cancellationToken)
    {
        var currentPath = Path.Join(dataDir, $"{FilePrefixes.Manifest}current");
        var pointerBytes = await File.ReadAllBytesAsync(currentPath, cancellationToken).ConfigureAwait(false);
        return Pointer.Read(pointerBytes);
    }

    internal static void ThrowIfFaulted(Exception? error)
    {
        if (error != null)
            ExceptionDispatchInfo.Capture(error).Throw();
    }

    internal static Task WaitUntilAsync<T>(T state, Func<T, bool> condition, CancellationToken cancellationToken) =>
        WaitUntilAsync(state, condition, TimeSpan.FromSeconds(5), cancellationToken);

    internal static Task WaitUntilAsync<T>(T state, Func<T, CancellationToken, ValueTask<bool>> condition, CancellationToken cancellationToken) =>
        WaitUntilAsync(state, condition, TimeSpan.FromSeconds(5), cancellationToken);

    private static async Task WaitUntilAsync<T>(T state, Func<T, bool> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (!condition(state))
        {
            if (Environment.TickCount64 >= deadline)
                throw new TimeoutException("Timed out waiting for manifest retention side effects.");

            await Task.Delay(25, cancellationToken).ConfigureAwait(false);
        }
    }

    private static async Task WaitUntilAsync<T>(T state, Func<T, CancellationToken, ValueTask<bool>> condition, TimeSpan timeout, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(condition);

        var deadline = Environment.TickCount64 + Convert.ToInt64(timeout.TotalMilliseconds);
        while (true)
        {
            var remainingMs = deadline - Environment.TickCount64;
            if (remainingMs <= 0)
                throw new TimeoutException("Timed out waiting for manifest retention side effects.");

            using var source = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            source.CancelAfter(TimeSpan.FromMilliseconds(remainingMs));

            try
            {
                var satisfied = await condition(state, source.Token).ConfigureAwait(false);
                if (satisfied)
                    return;
            }
            catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Timed out waiting for manifest retention side effects.");
            }

            remainingMs = deadline - Environment.TickCount64;
            if (remainingMs <= 0)
                throw new TimeoutException("Timed out waiting for manifest retention side effects.");

            var delayMs = remainingMs < 25 ? Convert.ToInt32(remainingMs) : 25;
            await Task.Delay(delayMs, cancellationToken).ConfigureAwait(false);
        }
    }
}
