using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Checks logger classification through interfaces, base classes and mixed inheritance.</summary>
[Immutable]
public sealed class LoggerInheritanceTests
{
    private static readonly string CasesName = typeof(Cases).FullName!.Replace('+', '/');

    private static readonly Lazy<LoggerScanResult> Findings = new(static () =>
        LoggerArchitectureScanner.Scan(Path.Join(AppContext.BaseDirectory, "Squirix.Server.UnitTests.dll")));

    private interface IFactoryLeaf : IFactoryRoot
    {
        int FactoryLeafMarker { get; }
    }

    private interface IFactoryRoot : ILoggerFactory
    {
        int FactoryRootMarker { get; }
    }

    private interface IGenericLeaf<out T> : IGenericRoot<T>
    {
        int GenericLeafMarker { get; }
    }

    private interface IGenericRoot<out T> : ILogger<T>
    {
        int GenericRootMarker { get; }
    }

    private interface IHybrid : IFactoryLeaf, ILeaf
    {
        int HybridMarker { get; }
    }

    private interface ILeaf : IMiddle
    {
        int LeafMarker { get; }
    }

    private interface IMiddle : IRoot
    {
        int MiddleMarker { get; }
    }

    private interface IProviderLeaf : IProviderRoot
    {
        int ProviderLeafMarker { get; }
    }

    private interface IProviderRoot : ILoggerProvider
    {
        int ProviderRootMarker { get; }
    }

    private interface IRoot : ILogger
    {
        int RootMarker { get; }
    }

    /// <summary>Only instance loggers are banned as static-class parameters, including inherited and generic loggers.</summary>
    /// <param name="method">The fixture method.</param>
    /// <param name="instanceLogger">Whether its argument is an instance logger.</param>
    [Test]
    [Arguments(nameof(Cases.ClassLogger), true)]
    [Arguments(nameof(Cases.InterfaceLogger), true)]
    [Arguments(nameof(Cases.GenericLogger), true)]
    [Arguments(nameof(Cases.HybridLogger), true)]
    [Arguments(nameof(Cases.Factory), false)]
    [Arguments(nameof(Cases.Provider), false)]
    [Arguments(nameof(Cases.Plain), false)]
    public async Task InheritedParameterKindsStayDistinct(string method, bool instanceLogger)
    {
        var prefix = $"{CasesName}.{method}:";
        var matches = 0;
        foreach (var finding in Findings.Value.StaticClassParameters)
        {
            if (finding.StartsWith(prefix, StringComparison.Ordinal))
                matches++;
        }

        _ = await Assert.That(matches).IsEqualTo(instanceLogger ? 1 : 0);
    }

    /// <summary>Factories and providers remain logger-like for static state, and hybrid loggers retain optional/nullable checks.</summary>
    [Test]
    public async Task OtherRulesRecognizeInheritedKinds()
    {
        _ = await Assert.That(Findings.Value.StaticMembers).Contains($"{CasesName}.{nameof(Cases.FactoryState)}: a static method must not return a logger.");
        _ = await Assert.That(Findings.Value.StaticMembers).Contains($"{CasesName}.{nameof(Cases.ProviderState)}: a static method must not return a logger.");
        _ = await Assert.That(Findings.Value.OptionalLoggers).Contains($"{CasesName}.{nameof(Cases.Optional)}: logger parameter 'logger' must not be optional.");
        _ = await Assert.That(Findings.Value.OptionalLoggers).Contains($"{CasesName}.{nameof(Cases.Optional)}: logger parameter 'logger' must not be nullable.");
    }

    private static class Cases
    {
        internal static void ClassLogger(ClassLeaf logger) => GC.KeepAlive(logger);

        internal static void Factory(IFactoryLeaf factory) => GC.KeepAlive(factory);

        internal static IFactoryLeaf? FactoryState() => null;

        internal static void GenericLogger(IGenericLeaf<string> logger) => GC.KeepAlive(logger);

        internal static void HybridLogger(IHybrid logger) => GC.KeepAlive(logger);

        internal static void InterfaceLogger(ILeaf logger) => GC.KeepAlive(logger);

        internal static void Optional(IHybrid? logger = null) => GC.KeepAlive(logger);

        internal static void Plain(object value) => GC.KeepAlive(value);

        internal static void Provider(IProviderLeaf provider) => GC.KeepAlive(provider);

        internal static IProviderLeaf? ProviderState() => null;
    }

    private abstract class ClassLeaf : ClassMiddle;

    private abstract class ClassMiddle : ClassRoot;

    private abstract class ClassRoot : ILogger
    {
        public abstract IDisposable? BeginScope<TState>(TState state)
            where TState : notnull;

        public abstract bool IsEnabled(LogLevel logLevel);

        public abstract void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter);
    }
}
