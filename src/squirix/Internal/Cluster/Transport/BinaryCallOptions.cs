using System;
using System.Threading;

namespace Squirix.Internal.Cluster.Transport;

internal readonly record struct BinaryCallOptions(DateTime Deadline, CancellationToken CancellationToken, CancellationToken CallerToken);
