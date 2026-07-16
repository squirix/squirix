using System;
using System.IO;
using ArchUnitNET.Loader;
using DomainArchitecture = ArchUnitNET.Domain.Architecture;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Cached ArchUnitNET model for the <c>Squirix.Server</c> assembly.</summary>
internal static class ServerArchitecture
{
    private static readonly Lazy<DomainArchitecture> LazyInstance = new(Build);

    internal static DomainArchitecture Instance => LazyInstance.Value;

    private static DomainArchitecture Build()
    {
        var directory = AppContext.BaseDirectory;
        var dllPath = Path.Join(directory, "Squirix.Server.dll");
        if (!File.Exists(dllPath))
        {
            throw new InvalidOperationException($"Expected Squirix.Server.dll in test output directory '{directory}'.");
        }

        return new ArchLoader()
            .LoadFilteredDirectory(directory, "Squirix.Server.dll")
            .Build();
    }
}
