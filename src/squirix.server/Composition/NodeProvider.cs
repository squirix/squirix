using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Composition;

/// <summary>Constructs each registered node singleton once and releases owned components in reverse construction order.</summary>
internal sealed class NodeProvider : IServiceProvider, IAsyncDisposable
{
    private readonly Lock _gate = new();
    private readonly FrozenDictionary<Type, List<Entry>> _services;
    private readonly HashSet<object> _tracked = [with(ReferenceEqualityComparer.Instance)];
    private readonly List<object> _disposables = [];
    private int _disposed;

    internal NodeProvider(NodeServices services)
    {
        var servicesByType = new Dictionary<Type, List<Entry>>();
        foreach (var registration in services.Registrations)
        {
            if (!servicesByType.TryGetValue(registration.Type, out var entries))
            {
                entries = [];
                servicesByType.Add(registration.Type, entries);
            }

            entries.Add(new Entry(registration));
            if (registration.Instance is IDisposable or IAsyncDisposable)
                _ = _tracked.Add(registration.Instance);
        }

        _services = servicesByType.ToFrozenDictionary();
    }

    public object? GetService(Type serviceType)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        if (!_services.TryGetValue(serviceType, out var entries))
            return null;

        var entry = entries[^1];
        if (entry.Created)
            return entry.Value;

        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return Resolve(entry);
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_gate)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
        }

        List<Exception>? failures = null;
        for (var index = _disposables.Count - 1; index >= 0; index--)
        {
            try
            {
                if (_disposables[index] is IAsyncDisposable asynchronous)
                    await asynchronous.DisposeAsync().ConfigureAwait(false);
                else if (_disposables[index] is IDisposable synchronous)
                    synchronous.Dispose();
            }
#pragma warning disable CA1031 // Release every component, including the directory lock, before reporting disposal failures.
            catch (Exception ex)
            {
                (failures ??= []).Add(ex);
            }
#pragma warning restore CA1031
        }

        if (failures is { Count: 1 })
            ExceptionDispatchInfo.Throw(failures[0]);
        if (failures != null)
            throw new AggregateException(failures);
    }

    internal T[] All<T>()
    {
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            if (!_services.TryGetValue(typeof(T), out var entries))
                return [];
            var items = new T[entries.Count];
            for (var index = 0; index < items.Length; index++)
                items[index] = Resolve(entries[index]) is T value ? value : throw new InvalidOperationException($"Invalid node component {typeof(T)}.");
            return items;
        }
    }

    private object? Resolve(Entry entry)
    {
        if (entry.Created)
            return entry.Value;

        var value = entry.Registration.Factory!(this);
        if (value is IDisposable or IAsyncDisposable && _tracked.Add(value))
            _disposables.Add(value);
        entry.Value = value;
        entry.Created = true;
        return value;
    }

    private sealed class Entry
    {
        internal Entry(NodeServices.Registration registration)
        {
            Registration = registration;
            Value = registration.Instance;
            Created = registration.Factory == null;
        }

        internal NodeServices.Registration Registration { get; }

        internal object? Value { get; set; }

        internal bool Created { get => Volatile.Read(ref field); set => Volatile.Write(ref field, value); }
    }
}
