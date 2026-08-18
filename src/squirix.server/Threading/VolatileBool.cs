using System.Threading;
using Squirix.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Holds a Boolean value with acquire/release semantics on every read and write.</summary>
/// <remarks>
/// The value is stored as an integer so that the memory barriers are explicit and
/// the wrapper can be used without volatile access in callers.
/// </remarks>
[ThreadSafe]
internal sealed class VolatileBool
{
    private int _value;

    internal bool Read() => Volatile.Read(ref _value) != 0;

    internal void Write(bool value) => Volatile.Write(ref _value, value ? 1 : 0);
}
