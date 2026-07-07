using System;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Pattern-based journal record enumeration without <see cref="System.Collections.Generic.IEnumerator{T}" />.</summary>
internal interface IJournalRecordEnumerator : IDisposable
{
    /// <summary>Gets the record at the current enumerator position.</summary>
    JournalRecord Current { get; }

    /// <summary>Advances the enumerator to the next record.</summary>
    /// <returns><see langword="true" /> when a record was positioned; otherwise <see langword="false" />.</returns>
    bool MoveNext();
}
