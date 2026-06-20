using System;
using Squirix.Server.Storage.Journaling.JsonFramed;

namespace Squirix.Server.Node.Services;

internal interface IJournalCompactionStatus
{
    bool IsInFlight { get; }

    DateTime LastRunUtc { get; }

    CompactionState State { get; }
}
