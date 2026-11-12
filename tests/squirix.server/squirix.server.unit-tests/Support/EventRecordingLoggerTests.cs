using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Guards event waits before, after and during concurrent logging.</summary>
[Immutable]
public sealed class EventRecordingLoggerTests
{
    /// <summary>An entry recorded before registration is still observed.</summary>
    /// <param name="fragment">The message fragment to match.</param>
    [Test]
    [Arguments("")]
    [Arguments("target")]
    public async Task ExistingEntryCompletesWait(string fragment)
    {
        var logger = new EventRecordingLogger();
        Write(logger, 1, "the target entry");

        var awaited = logger.WhenLoggedAsync(1, fragment);

        _ = await Assert.That(awaited.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Event ids and ordinal message fragments select independent waits.</summary>
    [Test]
    public async Task OnlyMatchingEntryCompletesWait()
    {
        var logger = new EventRecordingLogger();
        var awaited = logger.WhenLoggedAsync(1, "target");
        var other = logger.WhenLoggedAsync(1, "other");
        Write(logger, 2, "target");
        Write(logger, 1, "TARGET");
        _ = await Assert.That(awaited.IsCompleted).IsFalse();

        Write(logger, 1, "the target entry");

        _ = await Assert.That(awaited.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(other.IsCompleted).IsFalse();
        _ = await Assert.That(logger.Count(1)).IsEqualTo(2);
        _ = await Assert.That(logger.FindMessage(1)).IsEqualTo("TARGET");
        _ = await Assert.That(logger.WhenLoggedAsync(1, "target")).IsSameReferenceAs(awaited);
    }

    /// <summary>An event-only wait accepts any message for the requested id.</summary>
    [Test]
    public async Task EmptyFragmentMatchesFutureEntry()
    {
        var logger = new EventRecordingLogger();
        var awaited = logger.WhenLoggedAsync(1);
        Write(logger, 2, "unrelated");
        _ = await Assert.That(awaited.IsCompleted).IsFalse();

        Write(logger, 1, "entry");

        _ = await Assert.That(awaited.IsCompletedSuccessfully).IsTrue();
    }

    /// <summary>Concurrent registration and logging cannot lose the matching entry.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentRegistrationDoesNotLoseEntry(CancellationToken cancellationToken)
    {
        var logger = new EventRecordingLogger();
        Task? awaited = null;
        await ThreadRace.RunAsync(
            [
                () =>
                {
                    awaited = logger.WhenLoggedAsync(1, "target");
                    return Task.CompletedTask;
                },
                () =>
                {
                    Write(logger, 1, "target");
                    return Task.CompletedTask;
                },
            ],
            TimeSpan.FromSeconds(10),
            cancellationToken);

        _ = await Assert.That(awaited!.IsCompletedSuccessfully).IsTrue();
    }

    private static void Write(EventRecordingLogger logger, int eventId, string message) =>
        logger.Log(LogLevel.Information, new EventId(eventId), message, null, static (state, _) => state);
}
