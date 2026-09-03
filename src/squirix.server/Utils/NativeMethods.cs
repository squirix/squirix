using System;
using System.Reflection;
using System.Runtime.InteropServices;

namespace Squirix.Server.Utils;

/// <summary>Native method declarations for the server's low-level file and directory operations.</summary>
internal static partial class NativeMethods
{
    private const string LibcLibraryName = "libc";
    private const string DarwinSystemLibraryName = "libSystem.B.dylib";

    static NativeMethods()
    {
        NativeLibrary.SetDllImportResolver(typeof(NativeMethods).Assembly, ResolveLibc);
    }

    /// <summary>Opens a file descriptor through <c language="csharp">open(2)</c>.</summary>
    /// <param name="path">The NUL-terminated UTF-8 path bytes.</param>
    /// <param name="flags">The <c language="csharp">open(2)</c> flags. Creation flags such as <c language="csharp">O_CREAT</c> are not supported, because this declaration omits the variadic <c language="csharp">mode</c> argument.</param>
    /// <returns>The file descriptor, or a negative value on failure.</returns>
    /// <remarks>This import is valid on Unix platforms only.</remarks>
    [LibraryImport(LibcLibraryName, EntryPoint = "open", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int OpenDirectoryDescriptor([In] byte[] path, int flags);

    /// <summary>Resolves <c language="csharp">libc</c> imports on Apple platforms, where the BSD libc surface lives inside libSystem.</summary>
    /// <param name="libraryName">The library name requested by the P/Invoke declaration.</param>
    /// <param name="assembly">The assembly requesting the import.</param>
    /// <param name="searchPath">The default search path policy.</param>
    /// <returns>The loaded library handle, or <see cref="IntPtr.Zero" /> to fall back to default probing.</returns>
    private static IntPtr ResolveLibc(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (!string.Equals(libraryName, LibcLibraryName, StringComparison.Ordinal))
            return IntPtr.Zero;

        // macOS and Mac Catalyst ship no libc dylib to probe; Linux and FreeBSD keep their default libc probing.
        return NativeLibrary.TryLoad(DarwinSystemLibraryName, assembly, searchPath, out var handle) ? handle : IntPtr.Zero;
    }
}
