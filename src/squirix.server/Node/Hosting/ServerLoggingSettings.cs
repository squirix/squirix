using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

/// <summary>Reads the standalone console settings in precedence order and reloads the user's files after replacement.</summary>
internal sealed class ServerLoggingSettings : IDisposable
{
    private readonly string _defaults;
    private readonly string _application;
    private readonly string _environment;
    private readonly FileSystemWatcher _watcher;
    private readonly Timer _reload;
    private readonly Lock _gate = new();
    private Dictionary<string, string?> _values;
    private int _disposed;

    internal ServerLoggingSettings(string defaults, string directory, string environment, string? settingsFilePath = null)
    {
        _defaults = defaults;
        _application = settingsFilePath ?? Path.Join(directory, "appsettings.json");
        _environment = Path.Join(directory, $"appsettings.{environment}.json");
        _values = [with(StringComparer.OrdinalIgnoreCase)];
        _watcher = new FileSystemWatcher(directory);
        _watcher.Filters.Add(Path.GetFileName(_application));
        _watcher.Filters.Add(Path.GetFileName(_environment));
        _reload = new Timer(_ => Reload(), null, Timeout.Infinite, Timeout.Infinite);
        try
        {
            _watcher.Changed += QueueReload;
            _watcher.Created += QueueReload;
            _watcher.Deleted += QueueReload;
            _watcher.Renamed += OnRenamed;
            _watcher.Error += OnWatcherError;
            _watcher.EnableRaisingEvents = true;
            lock (_gate)
                _values = Read();
        }
        catch
        {
            Dispose();
            throw;
        }
    }

    internal event EventHandler? Changed;

    internal IReadOnlyDictionary<string, string?> Values => Volatile.Read(ref _values);

    public void Dispose()
    {
        lock (_gate)
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
        }

        _watcher.Changed -= QueueReload;
        _watcher.Created -= QueueReload;
        _watcher.Deleted -= QueueReload;
        _watcher.Renamed -= OnRenamed;
        _watcher.Error -= OnWatcherError;
        _watcher.Dispose();
        _reload.Dispose();
    }

    private static void MergeFile(Dictionary<string, string?> destination, string path)
    {
        try
        {
            using var handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
            var length = int.CreateChecked(RandomAccess.GetLength(handle));
            var bytes = ArrayPool<byte>.Shared.Rent(length);
            try
            {
                if (!HandleEx.TryReadExact(handle, bytes.AsSpan(0, length), 0))
                    throw new EndOfStreamException();
                using var json = JsonDocument.Parse(bytes.AsMemory(0, length), new JsonDocumentOptions { AllowTrailingCommas = true, CommentHandling = JsonCommentHandling.Skip });
                foreach (var property in json.RootElement.EnumerateObject())
                {
                    if (string.Equals(property.Name, "Logging", StringComparison.OrdinalIgnoreCase))
                        MergeSection(destination, string.Empty, property.Value);
                }
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(bytes);
            }
        }
        catch (Exception ex) when (ex is FileNotFoundException or DirectoryNotFoundException)
        {
            // Each settings file is optional.
        }
    }

    private static void MergeSection(Dictionary<string, string?> values, string prefix, JsonElement section)
    {
        if (section.ValueKind != JsonValueKind.Object)
            return;
        foreach (var property in section.EnumerateObject())
        {
            var key = string.Concat(prefix, property.Name);
            if (property.Value.ValueKind == JsonValueKind.Object)
                MergeSection(values, $"{key}:", property.Value);
            else
                values[key] = property.Value.ToString();
        }
    }

    private static void MergeEnvironment(Dictionary<string, string?> destination, string prefix)
    {
        foreach (DictionaryEntry entry in Environment.GetEnvironmentVariables())
        {
            if (entry.Key is not string name)
                continue;
            var normalized = name.Replace("__", ":", StringComparison.Ordinal);
            if (normalized.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
                destination[normalized[prefix.Length..]] = entry.Value as string;
        }
    }

    private Dictionary<string, string?> Read()
    {
        var values = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        MergeFile(values, _defaults);
        MergeEnvironment(values, "DOTNET_Logging:");
        MergeFile(values, _application);
        MergeFile(values, _environment);
        MergeEnvironment(values, "Logging:");
        return values;
    }

    private bool Tracks(string path) => string.Equals(path, _application, StringComparison.OrdinalIgnoreCase) || string.Equals(path, _environment, StringComparison.OrdinalIgnoreCase);

    private void QueueReload(object sender, FileSystemEventArgs args)
    {
        if (Tracks(args.FullPath))
            ScheduleReload();
    }

    private void OnRenamed(object sender, RenamedEventArgs args)
    {
        if (Tracks(args.OldFullPath) || Tracks(args.FullPath))
            ScheduleReload();
    }

    private void OnWatcherError(object sender, ErrorEventArgs args) => ScheduleReload();

    private void ScheduleReload()
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) == 0)
                _ = _reload.Change(TimeSpan.FromMilliseconds(250), Timeout.InfiniteTimeSpan);
        }
    }

    private void Reload()
    {
        lock (_gate)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return;
            try
            {
                Volatile.Write(ref _values, Read());
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException or JsonException or InvalidOperationException)
            {
                // A writer can briefly expose incomplete or inaccessible JSON. Keep the last valid filters.
                return;
            }
        }

        Changed?.Invoke(this, EventArgs.Empty);
    }
}
