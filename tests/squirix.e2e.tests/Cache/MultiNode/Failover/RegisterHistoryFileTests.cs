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
}
