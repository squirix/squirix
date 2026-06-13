using System;
using System.Diagnostics.CodeAnalysis;

namespace Squirix;

/// <summary>
/// Thrown when a cache mutation conflicts with an existing live entry.
/// </summary>
public sealed class CacheConflictException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="CacheConflictException" /> class.
    /// </summary>
    [SuppressMessage("ReSharper", "UnusedMember.Global", Justification = "Required public exception constructor for API compatibility.")]
    public CacheConflictException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CacheConflictException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    [SuppressMessage("ReSharper", "UnusedMember.Global", Justification = "Required public exception constructor for API compatibility.")]
    public CacheConflictException(string? message, Exception? innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="CacheConflictException" /> class for a conflicting key.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public CacheConflictException(string message)
        : base($"Cache entry '{message}' already exists.")
    {
        Key = message;
    }

    /// <summary>
    /// Gets the conflicting cache key.
    /// </summary>
    public string Key { get; private set; } = string.Empty;
}
