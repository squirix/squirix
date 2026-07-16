namespace Squirix.Server.Utils;

/// <summary>Mutable int holder so owning fields can stay readonly while Interlocked mutates the cell.</summary>
internal sealed class MutableInt32
{
    private int _value;

    internal ref int Value => ref _value;
}
