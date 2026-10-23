using Squirix.Server.Storage.Replication;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Reservation options for tests that do not care about the outcome byte bound: an outcome without a result is charged.</summary>
internal static class GroupReserveKit
{
    /// <summary>Gets the options of a new operation whose outcome carries no result.</summary>
    internal static GroupReserveOptions Unlogged { get; } = new(false, 0);

    /// <summary>Gets the options of an entry the log already holds, whose outcome carries no result.</summary>
    internal static GroupReserveOptions Logged { get; } = new(true, 0);
}
