using System;

namespace Squirix.Server.TestKit;

/// <summary>A component registration that a test can inspect or replace before startup.</summary>
/// <param name="ServiceType">The registered component type.</param>
/// <param name="ImplementationInstance">A caller-owned singleton instance.</param>
/// <param name="ImplementationFactory">The component factory.</param>
/// <param name="Scoped">Whether the instance belongs to a request.</param>
internal sealed record TestServiceRegistration(Type ServiceType, object? ImplementationInstance, Func<IServiceProvider, object?>? ImplementationFactory, bool Scoped = false)
{
    internal static TestServiceRegistration Singleton<T>(T instance) => new(typeof(T), instance, null);

    internal static TestServiceRegistration Singleton<T>(Func<IServiceProvider, T> factory) => new(typeof(T), null, provider => factory(provider));

    internal static TestServiceRegistration Singleton(Type type, Func<IServiceProvider, object?> factory) => new(type, null, factory);
}
