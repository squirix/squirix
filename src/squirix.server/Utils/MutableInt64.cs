namespace Squirix.Server.Utils;

/// <summary>Mutable long holder so owning fields can stay readonly while Interlocked mutates the cell.</summary>
internal sealed class MutableInt64
{
    private long _value;

    internal ref long Value => ref _value;
}
