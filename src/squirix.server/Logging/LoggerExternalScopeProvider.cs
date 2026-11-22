using System;
using System.Threading;

namespace Squirix.Server.Logging;

/// <summary>Tracks correlation scopes in the asynchronous flow of one node.</summary>
internal sealed class LoggerExternalScopeProvider
{
    private readonly AsyncLocal<Scope?> _current = new();

    internal IDisposable Push(object state)
    {
        var scope = new Scope(_current, _current.Value, state);
        _current.Value = scope;
        return scope;
    }

    internal void ForEachScope<T>(Action<object, T> visit, T target) => _current.Value?.Visit(visit, target);

    private sealed class Scope : IDisposable
    {
        private readonly AsyncLocal<Scope?> _current;
        private readonly Scope? _parent;
        private readonly object _state;

        internal Scope(AsyncLocal<Scope?> current, Scope? parent, object state)
        {
            _current = current;
            _parent = parent;
            _state = state;
        }

        public void Dispose()
        {
            if (ReferenceEquals(_current.Value, this))
                _current.Value = _parent;
        }

        internal void Visit<T>(Action<object, T> visit, T target)
        {
            _parent?.Visit(visit, target);
            visit(_state, target);
        }
    }
}
