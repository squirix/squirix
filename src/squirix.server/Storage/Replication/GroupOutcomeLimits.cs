using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>The bounds of a group idempotency store and the clock its ages are counted on.</summary>
/// <param name="Capacity">The maximum number of retained records that count against the capacity.</param>
/// <param name="Retention">How long a resolved outcome remains retrievable; <see cref="TimeSpan.MaxValue" /> for no limit.</param>
/// <param name="MinRetention">How long a resolved outcome is always retrievable.</param>
/// <param name="MaxOutcomeBytes">The most bytes the records are charged before a new operation needs an older outcome to make way.</param>
/// <param name="Clock">The clock the ages are counted on.</param>
[Immutable]
internal readonly record struct GroupOutcomeLimits(int Capacity, TimeSpan Retention, TimeSpan MinRetention, long MaxOutcomeBytes, TimeProvider Clock);
