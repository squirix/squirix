using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.InteropServices;
using Squirix.Attributes;

namespace Squirix.ProtocolModel;

[StructLayout(LayoutKind.Auto)]
[Immutable]
internal readonly struct LogEntry : IEquatable<LogEntry>
{
    internal LogEntry(int term, int index)
    {
        Term = term;
        Index = index;
    }

    internal int Term { get; }

    internal int Index { get; }

    public static bool operator ==(LogEntry left, LogEntry right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(LogEntry left, LogEntry right)
    {
        return !left.Equals(right);
    }

    public bool Equals(LogEntry other) => Term == other.Term && Index == other.Index;

    public override bool Equals([NotNullWhen(true)] object? obj) => obj is LogEntry other && Equals(other);

    public override int GetHashCode() => HashCode.Combine(Term, Index);
}
