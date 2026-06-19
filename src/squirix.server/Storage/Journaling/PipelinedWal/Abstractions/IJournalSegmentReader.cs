using System;
using System.Collections.Generic;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Reads decoded <see cref="JournalRecord"/> instances from a single on-disk journal segment.</summary>
internal interface IJournalSegmentReader : IEnumerable<JournalRecord>
{
    string Path { get; }

    bool TolerateTruncatedTail { get; }
}
