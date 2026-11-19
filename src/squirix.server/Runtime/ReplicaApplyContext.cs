using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Runtime;

/// <summary>Identifies writes whose durable source is the replica group log.</summary>
internal static class ReplicaApplyContext
{
    private static readonly AsyncLocal<bool> Active = new();

    internal static bool IsActive => Active.Value;

    internal static Scope Enter()
    {
        var previous = Active.Value;
        Active.Value = true;
        return new Scope(previous);
    }

    [Immutable]
    internal readonly record struct Scope(bool Previous) : IDisposable
    {
        public void Dispose() => Active.Value = Previous;
    }
}
