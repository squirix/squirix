using System;
using Squirix.Server.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.Server.UnitTests;

/// <summary>Caps concurrent unit tests to twice the machine processor count.</summary>
/// <remarks>
/// Many server unit tests spend most of their time waiting (expiry and stall timeouts, host and port setup), so an
/// exact processor-count cap leaves cores idle. Twice the count keeps the machine busy without the unbounded
/// oversubscription the integration and smoke suites guard against with <c language="csharp">ServerProcessorCountLimit</c>.
/// </remarks>
[Immutable]
public sealed class ServerUnitParallelLimit : IParallelLimit
{
    /// <inheritdoc />
    public int Limit => Environment.ProcessorCount * 2;
}
