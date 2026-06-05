using System;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Temporarily sets a process environment variable for E2E tests.
/// </summary>
internal sealed class E2ETempEnvironment : IDisposable
{
    private readonly string _key;
    private readonly string? _previous;
    private readonly string? _value;

    public E2ETempEnvironment(string key, string? value)
    {
        _key = key;
        _previous = Environment.GetEnvironmentVariable(key);
        _value = value;
        Environment.SetEnvironmentVariable(key, value);
    }

    public void Dispose()
    {
        var current = Environment.GetEnvironmentVariable(_key);
        if (string.Equals(current, _value, StringComparison.Ordinal))
            Environment.SetEnvironmentVariable(_key, _previous);
    }
}
