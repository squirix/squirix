using System;
using TUnit.Core.Interfaces;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Caps concurrent test execution to the machine processor count.</summary>
/// <remarks>Applied assembly-wide so parallel suites cannot oversubscribe the CPU without bound.</remarks>
public sealed class ProtocolModelProcessorCountLimit : IParallelLimit
{
    /// <inheritdoc />
    public int Limit => Environment.ProcessorCount;
}
