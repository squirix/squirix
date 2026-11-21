using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using Squirix.Server.Composition;

namespace Squirix.Server.TestKit;

/// <summary>Allows tests to override node registrations and add request-scoped probes.</summary>
internal sealed class TestServiceCollection : List<TestServiceRegistration>
{
    internal TestServiceCollection AddSingleton<T>(T instance)
    {
        Add(TestServiceRegistration.Singleton(instance));
        return this;
    }

    internal TestServiceCollection AddSingleton<T>(Func<IServiceProvider, T> factory)
    {
        Add(TestServiceRegistration.Singleton(factory));
        return this;
    }

    internal void AddDefaultSingleton<T>(Func<IServiceProvider, T> factory)
    {
        if (!Exists(static entry => entry.ServiceType == typeof(T)))
            Add(TestServiceRegistration.Singleton(factory));
    }

    internal TestServiceCollection AddScoped<T>(Func<IServiceProvider, T> factory)
    {
        Add(new TestServiceRegistration(typeof(T), null, provider => factory(provider), true));
        return this;
    }

    internal TestServiceCollection RemoveAll<T>()
    {
        _ = RemoveAll(static entry => entry.ServiceType == typeof(T));
        return this;
    }

    internal NodeProvider BuildServiceProvider()
    {
        var services = new NodeServices();
        var requestServices = new List<TestServiceRegistration>();
        foreach (var registration in this)
        {
            if (registration.Scoped)
                requestServices.Add(registration);
            else
                services.AddRegistration(new NodeServices.Registration(registration.ServiceType, registration.ImplementationInstance, registration.ImplementationFactory));
        }

        var registeredTypes = new HashSet<Type>();
        foreach (var registration in this)
            _ = registeredTypes.Add(registration.ServiceType);

        _ = services.Own<INodeRequestScopes>(provider => new RequestScopes(provider, registeredTypes.ToFrozenSet(), requestServices));
        return new NodeProvider(services);
    }

    private sealed class RequestScopes : INodeRequestScopes
    {
        private readonly IServiceProvider _root;
        private readonly FrozenSet<Type> _types;
        private readonly List<TestServiceRegistration> _registrations;

        internal RequestScopes(IServiceProvider root, FrozenSet<Type> types, List<TestServiceRegistration> registrations)
        {
            _root = root;
            _types = types;
            _registrations = registrations;
        }

        public bool Contains(Type type) => _types.Contains(type);

        public NodeRequestScope Open()
        {
            var provider = new RequestProvider(_root, _registrations);
            return new NodeRequestScope(provider, provider.Scope);
        }
    }

    private sealed class RequestProvider : IServiceProvider
    {
        private readonly IServiceProvider _root;
        private readonly FrozenSet<Type> _types;

        internal RequestProvider(IServiceProvider root, List<TestServiceRegistration> registrations)
        {
            _root = root;
            var services = new NodeServices();
            var types = new HashSet<Type>();
            foreach (var registration in CollectionsMarshal.AsSpan(registrations))
            {
                _ = types.Add(registration.ServiceType);
                services.AddRegistration(Bind(registration));
            }

            _types = types.ToFrozenSet();
            Scope = new NodeProvider(services);
        }

        internal NodeProvider Scope { get; }

        public object? GetService(Type serviceType) => _types.Contains(serviceType) ? Scope.GetService(serviceType) : _root.GetService(serviceType);

        private NodeServices.Registration Bind(TestServiceRegistration registration) =>
            new(registration.ServiceType, null, _ => registration.ImplementationFactory!(this));
    }
}
