using System;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Logger factory handing out one logger for every category; holds nothing that needs disposal.</summary>
[ThreadSafe]
internal sealed class FixedLoggerFactory : ILoggerFactory
{
    private static readonly ConditionalWeakTable<ILogger, ILoggerFactory> Factories = [];

    private readonly ILogger _logger;

    private FixedLoggerFactory(ILogger logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public void AddProvider(ILoggerProvider provider) => throw new NotSupportedException();

    /// <inheritdoc />
    public ILogger CreateLogger(string categoryName) => _logger;

    /// <inheritdoc />
    public void Dispose()
    {
    }

    /// <summary>Gets the factory that returns <paramref name="logger" /> for every category.</summary>
    /// <param name="logger">The logger returned for every category.</param>
    /// <returns>The factory; its disposal is a no-op.</returns>
    internal static ILoggerFactory For(ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(logger);
        return Factories.GetValue(logger, static l => new FixedLoggerFactory(l));
    }
}
