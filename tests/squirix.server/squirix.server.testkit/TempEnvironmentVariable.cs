using System;

namespace Squirix.Server.TestKit;

/// <summary>
/// Temporarily sets a process-level environment variable for the lifetime of this instance.
/// On disposal, restores the previous value (including <c>null</c> meaning "unset") if the
/// variable has not been changed by someone else in the meantime.
/// </summary>
/// <remarks>
///     <para>
///     This helper is intended for tests to isolate behavior driven by environment variables.
///     It updates the variable for the entire current process, which may affect other tests
///     running concurrently. Prefer serializing such tests or scoping them carefully.
///     </para>
///     <para>
///     The previous value is restored only if the variable still equals the value set by this
///     instance; if another component modifies the variable after construction, disposal will
///     not overwrite that external change.
///     </para>
/// </remarks>
/// <example>
///     <code>
/// using var _ = new TempEnvironmentVariable("SQUIRIX_JWT_AUDIENCE", "squirix-test");
/// // Run code that relies on SQUIRIX_JWT_AUDIENCE=squirix-test
/// </code>
/// </example>
public sealed class TempEnvironmentVariable : IDisposable
{
    private readonly string _key;
    private readonly string? _prev;
    private readonly string? _value;

    /// <summary>
    /// Initializes a new instance of the <see cref="TempEnvironmentVariable" /> class.
    /// </summary>
    /// <param name="key">Environment variable name (case-insensitive on Windows, case-sensitive on Unix).</param>
    /// <param name="value">
    /// Value to set for the duration of this instance. Use <c>null</c> to temporarily unset the variable.
    /// </param>
    public TempEnvironmentVariable(string key, string? value)
    {
        _key = key;
        _prev = Environment.GetEnvironmentVariable(key);
        _value = value;
        Environment.SetEnvironmentVariable(key, value);
    }

    /// <summary>
    /// Restores the previous value if the variable was not externally modified since construction.
    /// </summary>
    public void Dispose()
    {
        var current = Environment.GetEnvironmentVariable(_key);
        if (string.Equals(current, _value, StringComparison.Ordinal))
            Environment.SetEnvironmentVariable(_key, _prev);
    }
}
