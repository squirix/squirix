using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>
/// Holds a single <see cref="double" /> value with acquire/release semantics on every read and write.
/// The volatile barriers live in the <see cref="Read" /> and <see cref="Write" /> methods so that
/// callers (including property getters) can read or replace the value without embedding
/// <see cref="Volatile" /> barriers directly in their own method bodies.
/// </summary>
/// <remarks>
/// Value-type counterpart of <see cref="VolatileField{T}" />: the .NET generic
/// <see cref="Volatile" /> read/write overloads are reference-type only, so <see cref="double" /> is
/// wrapped around the dedicated non-generic <see cref="Volatile" /> double overloads. Like
/// <see cref="VolatileField{T}" />, this is a volatile value holder that keeps the memory barrier
/// out of property getters, satisfying analyzers such as NDepend ND1904.
/// </remarks>
[ThreadSafe]
internal sealed class VolatileDouble
{
    private double _value;

    internal double Read() => Volatile.Read(ref _value);

    internal void Write(double value) => Volatile.Write(ref _value, value);
}
