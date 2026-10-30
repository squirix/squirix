using System.Threading.Tasks;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.SourceScanning;

/// <summary>Guards the source checks against generic calls, whitespace and comment false positives.</summary>
public sealed class ReplicationSourceRulesTests
{
    /// <summary>Service resolution syntax is recognized in both generic and non-generic forms.</summary>
    /// <param name="source">The source fragment.</param>
    /// <param name="expected">Whether the fragment violates the rule.</param>
    [Test]
    [Arguments("provider.GetRequiredService<Foo>();", true)]
    [Arguments("provider . GetService \n <Foo>();", true)]
    [Arguments("GetServices<Foo>(provider);", true)]
    [Arguments("provider.GetService(typeof(Foo));", true)]
    [Arguments("IServiceScopeFactory factory;", true)]
    [Arguments("// provider.GetRequiredService<Foo>();", false)]
    [Arguments("var text = \"IServiceProvider\";", false)]
    public async Task ServiceResolutionIsDetected(string source, bool expected) =>
        _ = await Assert.That(ReplicationSourceRules.HasViolation(ReplicationSourceRules.ServiceLocator, source)).IsEqualTo(expected);

    /// <summary>Debug calls are detected while text mentions remain harmless.</summary>
    /// <param name="source">The source fragment.</param>
    /// <param name="expected">Whether the fragment violates the rule.</param>
    [Test]
    [Arguments("Console . WriteLine(1);", true)]
    [Arguments("Console.Error.WriteLine(1);", true)]
    [Arguments("value.Dump<int>();", true)]
    [Arguments("using Newtonsoft.Json;", true)]
    [Arguments("using System . Dynamic;", true)]
    [Arguments("/* Console.WriteLine(1); */", false)]
    [Arguments("var text = \"Dump(\";", false)]
    public async Task DebugOutputIsDetected(string source, bool expected) =>
        _ = await Assert.That(ReplicationSourceRules.HasViolation(ReplicationSourceRules.DebugOutput, source)).IsEqualTo(expected);
}
