using System;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Architecture rules that keep loggers explicit in the client SDK assembly.</summary>
[Immutable]
public sealed class ClientLoggerArchitectureTests
{
    private static readonly string AssemblyPath = PathKit.Combine(AppContext.BaseDirectory, "Squirix.dll");

    /// <summary>Ensures no static field, property or method holds or returns a logger.</summary>
    [Test]
    public async Task StaticMembersShouldNotHoldLoggers()
    {
        var violations = LoggerArchitectureScanner.FindStaticLoggerMembers(AssemblyPath);

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures static classes take no logger parameters apart from source-generated logging methods.</summary>
    [Test]
    public async Task StaticClassesShouldNotTakeLoggers()
    {
        var violations = LoggerArchitectureScanner.FindStaticClassLoggerParameters(AssemblyPath);

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    /// <summary>Ensures logger parameters, fields and properties are required and immutable.</summary>
    [Test]
    public async Task LoggersShouldBeRequired()
    {
        var violations = LoggerArchitectureScanner.FindOptionalLoggers(AssemblyPath);

        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }
}
