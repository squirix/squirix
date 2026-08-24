using System.Runtime.CompilerServices;
using Meziantou.Analyzer.Annotations;

[assembly: ExcludeFromBlockingCallAnalysis(typeof(System.Threading.CancellationTokenRegistration), "Dispose")]
[assembly: InternalsVisibleTo("Squirix.Server.UnitTests")]
[assembly: InternalsVisibleTo("Squirix.Server.IntegrationTests")]
[assembly: InternalsVisibleTo("Squirix.Server.SmokeTests")]
[assembly: InternalsVisibleTo("Squirix.Server.TestKit")]
[assembly: InternalsVisibleTo("squirix-test-host")]
[assembly: InternalsVisibleTo("sqr-ring-distribution")]
[assembly: InternalsVisibleTo("Squirix.Server.Benchmarks")]
[assembly: InternalsVisibleTo("DynamicProxyGenAssembly2")]
