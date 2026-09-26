using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;

namespace Squirix.Server.Utils;

/// <summary>Host logging wiring for <see cref="LogManager" />.</summary>
internal static partial class LogManager
{
    private static ILoggerFactory? HostFactory { get; set; }

    internal static void Configure(ILoggerFactory factory)
    {
        ArgumentNullException.ThrowIfNull(factory);
        HostFactory = factory;
    }

    /// <summary>Returns a logger that writes to the host logger factory configured when it logs.</summary>
    /// <typeparam name="T">The logger category type.</typeparam>
    /// <returns>The logger; it discards entries while no host logger factory is configured.</returns>
    /// <remarks>
    /// Components built while the host is composed (the journal, its event loop and durability pipeline, the replica group
    /// logs) take their logger before the host logger factory exists, so the logger resolves the factory on use, not here.
    /// </remarks>
    internal static ILogger<T> GetLogger<T>()
        where T : class => new HostLogger<T>();

    /// <summary>Returns a logger that writes to the host logger factory configured when it logs.</summary>
    /// <param name="categoryName">The logger category.</param>
    /// <returns>The logger; it discards entries while no host logger factory is configured.</returns>
    internal static ILogger GetLogger(string categoryName) => new NamedHostLogger(categoryName);

    /// <summary>
    /// Forwards to a logger of the currently configured host factory, created once per factory. Allocation-free while the
    /// factory stays the same; discards entries while none is configured or once the configured one is disposed.
    /// </summary>
    [ThreadSafe]
    private abstract class HostLogger : ILogger
    {
        private HostLoggerBinding? _binding;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => Resolve().BeginScope(state);

        public bool IsEnabled(LogLevel logLevel) => Resolve().IsEnabled(logLevel);

#pragma warning disable ZA0401 // Forwards an entry already built by a [LoggerMessage] method; nothing is formatted or boxed here.
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            Resolve().Log(logLevel, eventId, state, exception, formatter);
#pragma warning restore ZA0401

        protected abstract ILogger Create(ILoggerFactory factory);

        private ILogger Resolve()
        {
            var factory = HostFactory;
            if (factory == null)
                return NullLogger.Instance;

            var binding = Volatile.Read(ref _binding);
            return binding != null && ReferenceEquals(binding.Factory, factory) ? binding.Logger : Bind(factory);
        }

        private ILogger Bind(ILoggerFactory factory)
        {
            ILogger logger;
            try
            {
                logger = Create(factory);
            }
            catch (ObjectDisposedException)
            {
                // The host that supplied the factory has been torn down; diagnostics are best-effort.
                logger = NullLogger.Instance;
            }

            Volatile.Write(ref _binding, new HostLoggerBinding(factory, logger));
            return logger;
        }
    }

    /// <summary>Host logger for the category of <typeparamref name="T" />.</summary>
    /// <typeparam name="T">The logger category type.</typeparam>
    [ThreadSafe]
    private sealed class HostLogger<T> : HostLogger, ILogger<T>
        where T : class
    {
        protected override ILogger Create(ILoggerFactory factory) => factory.CreateLogger<T>();
    }

    /// <summary>Host logger for a named category.</summary>
    [ThreadSafe]
    private sealed class NamedHostLogger : HostLogger
    {
        private readonly string _categoryName;

        internal NamedHostLogger(string categoryName)
        {
            _categoryName = categoryName;
        }

        protected override ILogger Create(ILoggerFactory factory) => factory.CreateLogger(_categoryName);
    }

    /// <summary>A host logger factory paired with the logger it created, published together.</summary>
    [Immutable]
    private sealed class HostLoggerBinding
    {
        internal HostLoggerBinding(ILoggerFactory factory, ILogger logger)
        {
            Factory = factory;
            Logger = logger;
        }

        internal ILoggerFactory Factory { get; }

        internal ILogger Logger { get; }
    }
}
