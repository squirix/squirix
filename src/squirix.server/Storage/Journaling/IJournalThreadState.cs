using System;
using System.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>The journal thread and the failure latch that stops the durability pipeline.</summary>
internal interface IJournalThreadState
{
    /// <summary>Gets the journal thread.</summary>
    Thread JournalThread { get; }

    /// <summary>Returns the failure that stopped the journal thread, if any.</summary>
    /// <returns>The recorded failure, or <see langword="null" /> when the journal thread has not failed.</returns>
    Exception? GetJournalThreadFailure();

    /// <summary>Records the failure that stopped the journal thread; the first recorded failure wins.</summary>
    /// <param name="reason">The failure.</param>
    /// <returns><see langword="true" /> when this call recorded the failure; <see langword="false" /> when one was already recorded.</returns>
    bool TrySetJournalThreadFailure(Exception reason);
}
