using System.Diagnostics.CodeAnalysis;

#pragma warning disable IDE0076
[assembly:
    SuppressMessage(
        "NDepend",
        "ND2500:DontCreateThreadsExplicitly",
        Target = "Squirix.Server.Threading.SingleConsumerWorker<T>..ctor(Action<T>,Action<T,Exception>)",
        Justification = "TODO")]
[assembly:
    SuppressMessage(
        "NDepend",
        "ND1803:TypesThatCouldBeDeclaredAsPrivateNestedInAParentType",
        Target = "Squirix.Server.Utils.NativeMethods",
        Justification = "A class that contains native P/Invoke declarations must be a static partial class, so it cannot be a private nested type.")]
#pragma warning restore IDE0076
