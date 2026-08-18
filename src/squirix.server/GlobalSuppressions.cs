using System.Diagnostics.CodeAnalysis;

#pragma warning disable IDE0076
[assembly:
    SuppressMessage(
        "NDepend",
        "ND2500:DontCreateThreadsExplicitly",
        Target = "Squirix.Server.Threading.SingleConsumerWorker<T>..ctor(Action<T>,Action<T,Exception>)",
        Justification = "TODO")]
#pragma warning restore IDE0076
