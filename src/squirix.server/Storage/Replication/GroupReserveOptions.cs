using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>How a group idempotency reservation is taken.</summary>
/// <param name="AlreadyLogged">
/// <see langword="true" /> for an entry the log already holds, such as a recovered tail: it is pinned even past the capacity and the byte
/// bound, which bound new operations only, since refusing it would leave a logged entry unpinned and stop its group from starting.
/// </param>
/// <param name="OutcomeLength">The length of the outcome payload the entry will resolve with; the pin is charged its snapshot size.</param>
[Immutable]
internal readonly record struct GroupReserveOptions(bool AlreadyLogged, int OutcomeLength);
