using System;

namespace Squirix.Server.Storage.Journaling.Read;

/// <summary>Pattern-based enumeration of journal frame sequences without building records.</summary>
internal interface IJournalSequenceEnumerator : IDisposable
{
    /// <summary>Gets the sequence of the frame at the current enumerator position.</summary>
    ulong Current { get; }

    /// <summary>Advances the enumerator to the next frame.</summary>
    /// <returns><see langword="true" /> when a frame was positioned; otherwise <see langword="false" />.</returns>
    bool MoveNext();
}
