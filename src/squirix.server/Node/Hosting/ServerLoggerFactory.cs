using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Threading;
using Squirix.Server.Logging;

namespace Squirix.Server.Node.Hosting;

/// <summary>Creates the node's category loggers and refreshes console filters when configuration reloads.</summary>
internal sealed class ServerLoggerFactory : IServerLoggerFactory
{
    private readonly Lock _gate = new();
    private readonly ConcurrentDictionary<string, ConsoleLogger> _loggers = new(StringComparer.Ordinal);
    private readonly LoggerExternalScopeProvider _scopes = new();
    private readonly ServerLoggingSettings _settings;
    private readonly Action<string> _writeLine;

    internal ServerLoggerFactory(ServerLoggingSettings settings, Action<string> writeLine)
    {
        _settings = settings;
        _writeLine = writeLine;
        settings.Changed += Refresh;
    }

    public IServerLogger CreateLogger(string categoryName)
    {
        if (_loggers.TryGetValue(categoryName, out var existing))
            return existing;
        lock (_gate)
            return _loggers.GetOrAdd(categoryName, static (name, owner) => new ConsoleLogger(name, owner), this);
    }

    public void Dispose() => _settings.Changed -= Refresh;

    private void Refresh(object? sender, EventArgs args)
    {
        lock (_gate)
        {
            foreach (var entry in _loggers)
                entry.Value.Refresh();
        }
    }

    private sealed class ConsoleLogger : IServerLogger
    {
        private readonly string _name;
        private readonly ServerLoggerFactory _owner;
        private volatile bool _includeScopes;
        private volatile bool _json;
        private volatile ServerLogLevel _minimum;

        internal ConsoleLogger(string name, ServerLoggerFactory owner)
        {
            _name = name;
            _owner = owner;
            Refresh();
        }

        public bool IsScopeEnabled => _includeScopes;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => _includeScopes ? _owner._scopes.Push(state) : null;

        public bool IsEnabled(ServerLogLevel logLevel) => logLevel >= _minimum && logLevel < ServerLogLevel.None;

        public void Log<TState>(ServerLogLevel logLevel, ServerLogEventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (!IsEnabled(logLevel))
                return;

            if (_json)
            {
                WriteJson(logLevel, eventId, formatter(state, exception), state, exception);
                return;
            }

            var line = new StringBuilder();
            _ = line.Append(CultureInfo.InvariantCulture, $"{logLevel}: {_name}[{eventId.Id}] ");
            if (_includeScopes)
                _owner._scopes.ForEachScope(static (scope, text) => text.Append(scope).Append(' '), line);
            _ = line.Append(formatter(state, exception));
            if (exception != null)
                _ = line.AppendLine().Append(exception);

            _owner._writeLine(line.ToString());
        }

        internal void Refresh()
        {
            var settings = _owner._settings.Values;
            _json = string.Equals(settings.GetValueOrDefault("Console:FormatterName"), "json", StringComparison.OrdinalIgnoreCase);
            _minimum = FindLevel(settings, "Console:LogLevel:") ?? FindLevel(settings, "LogLevel:") ?? ServerLogLevel.Warning;
            _includeScopes = bool.TryParse(settings.GetValueOrDefault("Console:FormatterOptions:IncludeScopes") ?? settings.GetValueOrDefault("Console:IncludeScopes"), out var enabled) && enabled;
        }

        private static void WriteProperties(Utf8JsonWriter json, string? message, IReadOnlyList<KeyValuePair<string, object?>> fields)
        {
            json.WriteStartObject();
            json.WriteString("Message", message);
            for (var i = 0; i < fields.Count; i++)
            {
                var (name, value) = fields[i];
                WriteProperty(json, name, value);
            }

            json.WriteEndObject();
        }

        private static void WriteProperty(Utf8JsonWriter json, string name, object? value)
        {
            switch (value)
            {
                case null:
                    json.WriteNull(name);
                    break;
                case bool flag:
                    json.WriteBoolean(name, flag);
                    break;
                case double finite when double.IsFinite(finite):
                    json.WriteNumber(name, finite);
                    break;
                case float single when float.IsFinite(single):
                    json.WriteNumber(name, single);
                    break;
                default:
                    if (IsDecimalNumber(value))
                        json.WriteNumber(name, Convert.ToDecimal(value, CultureInfo.InvariantCulture));
                    else
                        json.WriteString(name, Convert.ToString(value, CultureInfo.InvariantCulture));
                    break;
            }
        }

        private static bool IsDecimalNumber(object value) =>
            value is byte or sbyte or short or ushort or int or uint or long or ulong or decimal;

        private static void WriteScope(object scope, Utf8JsonWriter writer)
        {
            var text = Convert.ToString(scope, CultureInfo.InvariantCulture);
            if (scope is IReadOnlyList<KeyValuePair<string, object?>> properties)
                WriteProperties(writer, text, properties);
            else
                writer.WriteStringValue(text);
        }

        private ServerLogLevel? FindLevel(IReadOnlyDictionary<string, string?> levels, string prefix)
        {
            ServerLogLevel? result = null;
            var length = -1;
            foreach (var section in levels)
            {
                if (!section.Key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                    continue;
                var key = section.Key[prefix.Length..];
                var specificity = string.Equals(key, "Default", StringComparison.OrdinalIgnoreCase) ? 0 : key.Length;
                if (specificity <= length || (specificity > 0 && !_name.StartsWith(key, StringComparison.OrdinalIgnoreCase)))
                    continue;
                if (!Enum.TryParse<ServerLogLevel>(section.Value, true, out var level))
                    continue;
                result = level;
                length = specificity;
            }

            return result;
        }

#pragma warning disable MA0045 // IServerLogger.Log is synchronous and this UTF-8 writer targets memory.
        private void WriteJson<TState>(ServerLogLevel level, ServerLogEventId id, string message, TState state, Exception? exception)
        {
            var buffer = new ArrayBufferWriter<byte>();
            using var json = new Utf8JsonWriter(buffer);
            json.WriteStartObject();
            json.WriteNumber("EventId", id.Id);
            json.WriteString("LogLevel", Enum.GetName(level));
            json.WriteString("Category", _name);
            json.WriteString("Message", message);
            if (exception != null)
                json.WriteString("Exception", exception.ToString());
            if (state is IReadOnlyList<KeyValuePair<string, object?>> fields)
            {
                json.WritePropertyName("State");
                WriteProperties(json, message, fields);
            }

            if (_includeScopes)
            {
                json.WriteStartArray("Scopes");
                _owner._scopes.ForEachScope(WriteScope, json);
                json.WriteEndArray();
            }

            json.WriteEndObject();
            json.Flush();
            _owner._writeLine(Encoding.UTF8.GetString(buffer.WrittenSpan));
        }

#pragma warning restore MA0045
    }
}
