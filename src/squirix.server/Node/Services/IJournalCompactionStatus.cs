using System;
using Squirix.Server.Storage.Journaling.Compaction;

namespace Squirix.Server.Node.Services;

internal interface IJournalCompactionStatus
{
    bool IsInFlight { get; }

    DateTime LastRunUtc { get; }

    RunState State { get; }
}
