using System;
using System.Collections.Generic;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace Squirix.Server.Logging;

/// <summary>Describes a typed node event and its ordered structured fields.</summary>
internal sealed class LogEvent
{
    private readonly string[] _names;
    private readonly CompositeFormat _format;

    internal LogEvent(int id, string name, string template)
    {
        Id = new ServerLogEventId(id, name);
        Template = template;
        var names = new List<string>();
        var format = new StringBuilder(template.Length);
        var i = 0;
        while (i < template.Length)
        {
            var ch = template[i++];
            _ = format.Append(ch);
            if (ch != '{')
                continue;
            if (i < template.Length && template[i] == '{')
            {
                _ = format.Append(template[i++]);
                continue;
            }

            var start = i;
            var end = template.AsSpan(start).IndexOfAny(',', ':', '}');
            if (end <= 0)
                throw new FormatException("The log template contains an invalid field.");

            _ = format.Append(names.Count.ToString(CultureInfo.InvariantCulture));
            names.Add(template.AsSpan(start, end).ToString());
            i += end;
        }

        _format = CompositeFormat.Parse(format.ToString());
        _names = [.. names];
    }

    internal ServerLogEventId Id { get; }

    internal string Template { get; }

    internal int FieldCount => _names.Length;

    internal void Write<T>(IServerLogger logger, ServerLogLevel level, Exception? exception, T values)
        where T : struct, ITuple
    {
        var state = new LogEventState<T>(this, values);
        logger.Log(level, Id, state, exception, static (entry, _) => entry.ToString());
    }

    internal string FieldName(int index) => _names[index];

    internal string Format<T>(T values)
        where T : struct, ITuple
    {
        if (values.Length == 0)
            return Template;

        var printable = new object?[values.Length];
        for (var i = 0; i < printable.Length; i++)
            printable[i] = values[i] ?? "(null)";

        return new StringBuilder(Template.Length).AppendFormat(CultureInfo.InvariantCulture, _format, printable).ToString();
    }
}
