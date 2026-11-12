using System;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Squirix.E2ETests.Fixtures;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>Saved history evidence preserves ambiguous outcomes, call intervals and consistency verdicts.</summary>
public sealed class RegisterHistoryFileTests
{
    /// <summary>Cleanup preserves a controller failure, while independent workload cancellation still propagates.</summary>
    /// <param name="alreadyFailed">Whether a controller error is already propagating.</param>
    /// <exception cref="InvalidOperationException">The simulated controller failure, caught by the test.</exception>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task CleanupPreservesFailureAndHistory(bool alreadyFailed)
    {
        var directory = Directory.CreateTempSubdirectory("squirix-cleanup-");
        try
        {
            var path = Path.Join(directory.FullName, "history.json");
            var history = new RegisterHistory();
            history.RecordWrite(new RegisterWrite("k", 1, 10, 20, true));
            var primary = new InvalidOperationException("controller aborted");
            var running = Task.FromCanceled(new CancellationToken(true));
            Exception? observed = null;
            try
            {
                try
                {
                    if (alreadyFailed)
                        throw primary;
                }
                finally
                {
                    await ProcessChaosTests.FinishWorkloadAsync(running, alreadyFailed, (path, history));
                }
            }
            catch (Exception exception) when (exception is InvalidOperationException or OperationCanceledException)
            {
                observed = exception;
            }

            _ = await Assert.That(alreadyFailed ? ReferenceEquals(observed, primary) : observed is OperationCanceledException).IsTrue();
            using var json = JsonDocument.Parse(await File.ReadAllBytesAsync(path, CancellationToken.None));
            _ = await Assert.That(json.RootElement.GetProperty("writes")[0][4].GetBoolean()).IsTrue();
        }
        finally
        {
            directory.Delete(true);
        }
    }

    /// <summary>The same artifact can reproduce both a legal late write and the resurrection anomaly.</summary>
    /// <param name="finalValue">The final observation.</param>
    /// <param name="valid">Whether the saved history is linearizable.</param>
    /// <param name="cancellationToken">Test cancellation token.</param>
    [Test]
    [Arguments(1L, true)]
    [Arguments(2L, false)]
    public async Task ReplayPreservesVerdict(long finalValue, bool valid, CancellationToken cancellationToken)
    {
        var directory = Directory.CreateTempSubdirectory("squirix-history-");
        try
        {
            var path = Path.Join(directory.FullName, "history.json");
            var history = new RegisterHistory();
            history.RecordWrite(new RegisterWrite("k", 1, 10, 20, false));
            history.RecordWrite(new RegisterWrite("k", 2, 30, 40, true));
            history.RecordRead(new RegisterRead("k", 50, 60, 2));
            history.RecordRead(new RegisterRead("k", 70, 80, 1));
            history.RecordRead(new RegisterRead("k", 90, 100, finalValue));
            history.RecordReadFailure("k", 101, 102);
            await RegisterHistoryFile.SaveAsync(path, history, cancellationToken);

            _ = await Assert.That((await RegisterHistoryFile.CheckAsync(path, cancellationToken)).Count == 0).IsEqualTo(valid);
            using var json = JsonDocument.Parse(await File.ReadAllBytesAsync(path, cancellationToken));
            var root = json.RootElement;
            _ = await Assert.That(root.GetProperty("writes")[0][4].GetBoolean()).IsFalse();
            _ = await Assert.That(root.GetProperty("writes")[1][4].GetBoolean()).IsTrue();
            _ = await Assert.That(root.GetProperty("failedReads").GetInt32()).IsEqualTo(1);
            _ = await Assert.That(root.GetProperty("failedReadCalls")[0][1].GetInt64()).IsEqualTo(101L);
            _ = await Assert.That(root.GetProperty("failedReadCalls")[0][2].GetInt64()).IsEqualTo(102L);
        }
        finally
        {
            directory.Delete(true);
        }
    }

    /// <summary>Empty histories and histories spanning several output buffers preserve every event in order.</summary>
    /// <param name="count">The number of events in each category.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1024)]
    public async Task ExportPreservesAllEvents(int count, CancellationToken cancellationToken)
    {
        var directory = Directory.CreateTempSubdirectory("squirix-history-export-");
        try
        {
            var path = Path.Join(directory.FullName, "history.json");
            var history = new RegisterHistory();
            const string key = "key-ключ-\"";
            for (var i = 0; i < count; i++)
            {
                history.RecordWrite(new RegisterWrite(key, i + 1L, i * 4L, (i * 4L) + 1, i % 2 == 0));
                history.RecordRead(new RegisterRead(key, (i * 4L) + 2, (i * 4L) + 3, i + 1L));
                history.RecordReadFailure(key, (i * 4L) + 4, (i * 4L) + 5);
            }

            await File.WriteAllTextAsync(path, new string('x', 256 * 1024), cancellationToken);
            await RegisterHistoryFile.SaveAsync(path, history, cancellationToken);

            using var json = JsonDocument.Parse(await File.ReadAllBytesAsync(path, cancellationToken));
            var root = json.RootElement;
            _ = await Assert.That(root.GetProperty("version").GetInt32()).IsEqualTo(1);
            _ = await Assert.That(root.GetProperty("failedReads").GetInt32()).IsEqualTo(count);
            var writes = root.GetProperty("writes");
            var reads = root.GetProperty("reads");
            var failed = root.GetProperty("failedReadCalls");
            _ = await Assert.That((writes.GetArrayLength(), reads.GetArrayLength(), failed.GetArrayLength())).IsEqualTo((count, count, count));
            for (var i = 0; i < count; i++)
            {
                var write = writes[i];
                var read = reads[i];
                var failure = failed[i];
                _ = await Assert.That((write[0].GetString(), write[1].GetInt64(), write[2].GetInt64(), write[3].GetInt64(), write[4].GetBoolean()))
                    .IsEqualTo((key, i + 1L, i * 4L, (i * 4L) + 1, i % 2 == 0));
                _ = await Assert.That((read[0].GetString(), read[1].GetInt64(), read[2].GetInt64(), read[3].GetInt64()))
                    .IsEqualTo((key, (i * 4L) + 2, (i * 4L) + 3, i + 1L));
                _ = await Assert.That((failure[0].GetString(), failure[1].GetInt64(), failure[2].GetInt64()))
                    .IsEqualTo((key, (i * 4L) + 4, (i * 4L) + 5));
            }
        }
        finally
        {
            directory.Delete(true);
        }
    }
}
