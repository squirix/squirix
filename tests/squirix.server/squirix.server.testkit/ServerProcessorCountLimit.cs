using System;
using Squirix.Server.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.Server.TestKit;

/// <summary>Caps concurrent test execution to the machine processor count.</summary>
/// <remarks>Applied assembly-wide so parallel suites cannot oversubscribe the CPU without bound.</remarks>
[Immutable]
public sealed class ServerProcessorCountLimit : IParallelLimit
{
    /// <inheritdoc />
    public int Limit => Environment.ProcessorCount;
}
