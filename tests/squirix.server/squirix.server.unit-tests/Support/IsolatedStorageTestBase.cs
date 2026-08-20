using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Base for unit tests that need a fresh temporary storage directory for the test class lifetime.
/// Implements <see cref="IAsyncLifetime" />: creates the directory in <see cref="InitializeAsync" /> and
/// disposes it when the class fixture tears down, exposing it to tests via <see cref="Dir" />.
/// </summary>
[Immutable]
public abstract class IsolatedStorageTestBase : ServerUnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    /// <summary>Gets the test's fresh temporary storage directory.</summary>
    /// <exception cref="InvalidOperationException">Thrown when accessed before <see cref="InitializeAsync" /> has run.</exception>
    protected TempDirectory Dir => _dir ?? throw new InvalidOperationException("Test directory is not initialized.");

    /// <summary>Gets the hint used when creating the temporary storage directory.</summary>
    protected virtual string TempDirectoryName => "squirix";

    /// <summary>Disposes the temporary directory after the test class finishes.</summary>
    public ValueTask DisposeAsync()
    {
        _dir?.Dispose();
        GC.SuppressFinalize(this);
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates a fresh temporary storage directory before the test class runs.</summary>
    public virtual ValueTask InitializeAsync()
    {
        _dir = new TempDirectory(TempDirectoryName);
        return ValueTask.CompletedTask;
    }
}
