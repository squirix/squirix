using System;
using System.Collections.Generic;

namespace Squirix.Server.Node.Hosting.Security;

internal sealed class ApiKeyAuthSettings
{
    private readonly HashSet<string> _allowedKeys;

    public ApiKeyAuthSettings(IEnumerable<string> keys)
    {
        _allowedKeys = new HashSet<string>(StringComparer.Ordinal);

        foreach (var key in keys)
        {
            if (string.IsNullOrWhiteSpace(key))
                continue;

            _ = _allowedKeys.Add(key.Trim());
        }
    }

    public bool IsEnabled => _allowedKeys.Count > 0;

    public bool IsAllowed(string? key) => key is not null && _allowedKeys.Contains(key);
}
