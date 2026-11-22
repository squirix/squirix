using System;

namespace Squirix.Server.Node.Observability;

internal sealed record ReadinessCheck(string Name, Func<IServiceProvider, IReadinessCheck> Create);
