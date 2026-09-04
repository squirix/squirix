using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.IO;
using Squirix.Server.Utils;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Base for unit tests that need a fresh temporary storage directory for the test class lifetime.
/// Inherits disposal plumbing from <see cref="DisposableServerUnitTestBase" /> and exposes the directory via <see cref="Dir" />.
/// </summary>
[Immutable]
public abstract class IsolatedStorageTestBase : DisposableServerUnitTestBase
{
    private TempDirectory? _dir;

    /// <summary>Gets the test's fresh temporary storage directory.</summary>
    /// <exception cref="InvalidOperationException">Thrown when accessed before <see cref="OnInitializeAsync" /> has run.</exception>
    protected TempDirectory Dir => ThrowHelper.Required(_dir, "Test directory is not initialized.");

    /// <summary>Gets the hint used when creating the temporary storage directory.</summary>
    protected virtual string TempDirectoryName => "squirix";

    /// <summary>Releases the temporary storage directory after the test class finishes.</summary>
    protected override void DisposeManaged() => _dir?.Dispose();

    /// <summary>Creates a fresh temporary storage directory before the test class runs.</summary>
    protected override ValueTask OnInitializeAsync()
    {
        _dir = new TempDirectory(TempDirectoryName);
        return ValueTask.CompletedTask;
    }
}
