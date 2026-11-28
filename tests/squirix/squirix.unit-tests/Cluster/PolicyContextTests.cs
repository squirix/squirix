using System;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Reliability;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Cluster;

/// <summary>Checks execution-context isolation when suspended SDK policies reuse pooled storage.</summary>
public sealed class PolicyContextTests
{
    /// <summary>Overlaps callers with different ambient contexts and repeats the sequence after the boxes are returned.</summary>
    [Test]
    public async Task ConcurrentCallsKeepTheirOwnContext()
    {
        var context = new AsyncLocal<string?>();
        var pending = new Task<string?>[144];
        var expected = new string[pending.Length];
        for (var round = 0; round < 2; round++)
        {
            var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            for (var index = 0; index < pending.Length; index++)
            {
                var value = ((round * pending.Length) + index).ToString(CultureInfo.InvariantCulture);
                context.Value = value;
                expected[index] = value;
                pending[index] = ObserveAsync(gate.Task, context).AsTask();
            }

            context.Value = "completer";
            gate.SetResult();
            var observed = await Task.WhenAll(pending);
            _ = await Assert.That(observed.AsSpan().SequenceEqual(expected)).IsTrue();
            _ = await Assert.That(context.Value).IsEqualTo("completer");
        }
    }

    [AsyncMethodBuilder(typeof(PolicyMethodBuilder<>))]
    private static async ValueTask<string?> ObserveAsync(Task gate, AsyncLocal<string?> context)
    {
        await gate.ConfigureAwait(false);
        var value = context.Value;
        context.Value = "finished";
        return value;
    }
}
