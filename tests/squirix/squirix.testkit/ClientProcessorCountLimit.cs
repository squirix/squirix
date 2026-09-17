using System;
using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.TestKit;

/// <summary>Caps concurrent test execution to the machine processor count.</summary>
/// <remarks>Applied assembly-wide so parallel suites cannot oversubscribe the CPU without bound.</remarks>
[Immutable]
public sealed class ClientProcessorCountLimit : IParallelLimit
{
    /// <inheritdoc />
    public int Limit => Environment.ProcessorCount;
}
