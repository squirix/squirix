using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>
/// Holds a single reference value with acquire/release semantics on every read and write.
/// The volatile barriers live in the <see cref="Read" /> and <see cref="Write" /> methods so that
/// callers (including property getters) can read or replace the value without embedding
/// <see cref="Volatile" /> barriers directly in their own method bodies.
/// </summary>
/// <remarks>
/// This is a volatile value holder, a synchronization primitive in the spirit of Java's
/// <c language="csharp">AtomicReference&lt;T&gt;</c> or a .NET atomic field wrapper. Centralizing the acquire/release
/// barriers at one point (1) documents the memory-visibility contract for the held value and
/// (2) keeps volatile access out of property getters, which satisfies analyzers that flag
/// volatile reads inside property accessors (for example NDepend ND1904).
/// </remarks>
/// <typeparam name="T">A reference type. Use <see cref="VolatileDouble" /> for <see cref="double" /> values.</typeparam>
[ThreadSafe]
internal sealed class VolatileField<T>
    where T : class?
{
    private T? _value;

    internal T? Read() => Volatile.Read(ref _value);

    internal void Write(T? value) => Volatile.Write(ref _value, value);
}
