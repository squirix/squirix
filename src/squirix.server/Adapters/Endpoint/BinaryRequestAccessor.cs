using System.Threading;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Supplies the active remote request to the admission gate within an asynchronous operation.</summary>
internal sealed class BinaryRequestAccessor
{
    private readonly AsyncLocal<Holder?> _active = new();

    internal BinaryRequestContext? Current
    {
        get => _active.Value?.Context;
        set => SetCurrent(value);
    }

    private void SetCurrent(BinaryRequestContext? value)
    {
        if (_active.Value is { } previous)
            previous.Context = null;
        _active.Value = value == null ? null : new Holder { Context = value };
    }

    private sealed class Holder
    {
        internal BinaryRequestContext? Context { get; set; }
    }
}
