using System;
using Squirix.Server.Storage.Journaling;

namespace Squirix.Server.Node.Services;

internal interface IJournalCompactionStatus
{
    bool IsInFlight { get; }

    DateTime LastRunUtc { get; }

    CompactionState State { get; }
}
