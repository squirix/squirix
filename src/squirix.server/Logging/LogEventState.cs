using System.Collections;
using System.Collections.Generic;
using System.Runtime.CompilerServices;

namespace Squirix.Server.Logging;

/// <summary>Provides the original field values and template to structured sinks.</summary>
/// <typeparam name="T">Tuple of event arguments in template order.</typeparam>
internal readonly record struct LogEventState<T> : IReadOnlyList<KeyValuePair<string, object?>>
    where T : struct, ITuple
{
    private readonly LogEvent _event;
    private readonly T _values;

    internal LogEventState(LogEvent descriptor, T values)
    {
        _event = descriptor;
        _values = values;
    }

    public int Count => _event.FieldCount + 1;

    public KeyValuePair<string, object?> this[int index] => index == _event.FieldCount
        ? new("{OriginalFormat}", _event.Template)
        : new(_event.FieldName(index), _values[index]);

    public override string ToString() => _event.Format(_values);

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator()
    {
        for (var i = 0; i < Count; i++)
            yield return this[i];
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
