using System.Threading;
using Squirix.Server.Core;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Supplies the active remote request to the admission gate within an asynchronous operation.</summary>
internal sealed class BinaryRequestAccessor : IRoutedKeyContext
{
    private readonly AsyncLocal<Holder?> _active = new();

    internal BinaryRequestContext? Current
    {
        get => _active.Value?.Context;
        set => SetCurrent(value);
    }

    public void RecordRoute(string cacheName, string key, ServerKeyValidationError? error, string? groupId) =>
        Current?.RecordRoute(cacheName, key, error, groupId);

    public bool TryGet(string? key, out ServerKeyValidationError? error)
    {
        if (Current is { } context)
            return context.TryGetKeyValidation(key, out error);

        error = null;
        return false;
    }

    public string? FindGroup(string cacheName, string key) => Current?.FindGroup(cacheName, key);

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
