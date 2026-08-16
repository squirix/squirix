using Squirix.Attributes;

namespace Squirix.Server.Storage.Journaling.Read;

[Immutable]
internal sealed record JournalFrameReadResult(JournalFrameReadStatus Status, long NextFrameOffset);
