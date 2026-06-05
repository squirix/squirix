using System;
using Squirix.Server.Core;

namespace Squirix.Server.Node.App.Decorators.Validation;

/// <summary>
/// Validates logical cache key strings before operations reach the inner pipeline.
/// </summary>
internal static class KeyInputValidator
{
    /// <summary>
    /// Validates a key string and throws <see cref="ArgumentException" /> when invalid.
    /// </summary>
    /// <param name="key">The key to validate.</param>
    /// <param name="parameterName">The caller parameter name for exceptions.</param>
    public static void Validate(string key, string parameterName) => _ = CacheKeyValidator.Validate(key, parameterName);
}
