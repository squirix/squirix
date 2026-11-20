using System;
using System.Collections.Generic;

namespace Squirix.Server.Composition;

/// <summary>The singleton instances and explicit factories used to construct one node.</summary>
internal sealed class NodeServices
{
    private readonly List<Registration> _registrations = [];

    internal IReadOnlyList<Registration> Registrations => _registrations;

    internal NodeServices Use<T>(T instance)
        where T : notnull
    {
        _registrations.Add(new Registration(typeof(T), instance, null));
        return this;
    }

    internal NodeServices Own<T>(Func<IServiceProvider, T> factory)
    {
        _registrations.Add(new Registration(typeof(T), null, provider => factory(provider)));
        return this;
    }

    internal void UseDefault<T>(T instance)
        where T : notnull
    {
        for (var index = 0; index < _registrations.Count; index++)
        {
            if (_registrations[index].Type == typeof(T))
                return;
        }

        _ = Use(instance);
    }

    internal NodeServices ReplaceFactory<T>(Func<IServiceProvider, T> factory)
    {
        _ = _registrations.RemoveAll(static entry => entry.Type == typeof(T));
        return Own(factory);
    }

    internal NodeServices RunService<T>(Func<IServiceProvider, T> factory)
        where T : INodeService => Own<INodeService>(provider => factory(provider));

    internal sealed record Registration(Type Type, object? Instance, Func<IServiceProvider, object?>? Factory);
}
