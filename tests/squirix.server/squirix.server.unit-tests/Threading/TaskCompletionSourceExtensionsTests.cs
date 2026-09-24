using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>A completion source is completed by the outcome of the operation that fulfils it.</summary>
[Immutable]
public sealed class TaskCompletionSourceExtensionsTests
{
    /// <summary>A failing operation faults the source with the same failure and rethrows it.</summary>
    [Test]
    public async Task RunFaultsSource()
    {
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new IOException("disk failed");

        var thrown = NodeExceptionAssert.For<IOException>().Throws((source, failure), static s => s.source.Run(s.failure, static f => throw f));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(source.Task.IsFaulted).IsTrue();
        _ = await Assert.That(source.Task.Exception!.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>A successful operation completes the source.</summary>
    [Test]
    public async Task RunCompletesSource()
    {
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        source.Run(0, static _ => { });

        _ = await Assert.That(source.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>A failing operation faults the source without throwing.</summary>
    [Test]
    public async Task RunIsolatedFaultsSource()
    {
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var failure = new InvalidOperationException("handler failed");

        source.RunIsolated(failure, static f => throw f);

        _ = await Assert.That(source.Task.Exception!.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>A successful operation completes the source.</summary>
    [Test]
    public async Task RunIsolatedCompletesSource()
    {
        var source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        source.RunIsolated(0, static _ => { });

        _ = await Assert.That(source.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Faulting a list faults every pending source with the same failure and leaves completed ones untouched.</summary>
    [Test]
    public async Task FaultAllFaultsPendingSources()
    {
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        completed.SetResult();
        var failure = new IOException("drain");
        List<TaskCompletionSource> sources = [pending, completed];

        sources.FaultAll(failure);

        _ = await Assert.That(pending.Task.Exception!.InnerException).IsSameReferenceAs(failure);
        _ = await Assert.That(completed.Task.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Completing a list completes every pending source and leaves faulted ones untouched.</summary>
    [Test]
    public async Task CompleteAllCompletesPendingSources()
    {
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var faulted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        faulted.SetException(new IOException("earlier"));
        List<TaskCompletionSource> sources = [pending, faulted];

        sources.CompleteAll();

        _ = await Assert.That(pending.Task.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(faulted.Task.IsFaulted).IsTrue();
    }
}
