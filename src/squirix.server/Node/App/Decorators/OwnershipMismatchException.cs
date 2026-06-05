using System;

namespace Squirix.Server.Node.App.Decorators;

internal sealed class OwnershipMismatchException : InvalidOperationException
{
    public OwnershipMismatchException(string operation, string cacheName, string key, string expectedOwner, string actualNode)
        : base(
            $"Ownership mismatch for local physical cache operation '{operation}' on cache '{cacheName}' and key '{key}'. " +
            $"Expected owner '{expectedOwner}', current node '{actualNode}'.")
    {
    }
}
