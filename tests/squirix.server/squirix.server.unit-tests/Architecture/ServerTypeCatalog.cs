using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Source-scan helpers for <c>Squirix.Server</c> type placement architecture rules.</summary>
internal static class ServerTypeCatalog
{
    private static readonly FrozenSet<string> DeclarationModifiers = FrozenSet.ToFrozenSet(
        [
            "public",
            "internal",
            "private",
            "protected",
            "file",
            "abstract",
            "sealed",
            "static",
            "partial",
            "readonly",
            "ref",
            "unsafe",
            "new",
        ],
        StringComparer.Ordinal);

    private static readonly FrozenSet<string>.AlternateLookup<ReadOnlySpan<char>> DeclarationModifierLookup = DeclarationModifiers.GetAlternateLookup<ReadOnlySpan<char>>();

    private static readonly Lock ScanGate = new();

    private static Task<IReadOnlyList<DeclaredType>>? _allDeclaredTypesTask;

    /// <summary>
    /// Collects declared types under <c>src/squirix.server</c> whose simple name ends with <paramref name="suffix" />.
    /// </summary>
    /// <param name="suffix">Required simple-name suffix.</param>
    /// <param name="includeInterfaces">When <see langword="false" />, interface declarations are omitted.</param>
    /// <param name="excludeFullNames">Exact <c>namespace.type</c> names to skip.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Matching type descriptors.</returns>
    internal static async Task<List<DeclaredType>> TypesWithNameEndingWithAsync(
        string suffix,
        bool includeInterfaces,
        string[]? excludeFullNames = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(suffix);
        excludeFullNames ??= [];

        cancellationToken.ThrowIfCancellationRequested();
        var declaredTypes = await AllDeclaredTypesAsync().WaitAsync(cancellationToken);

        var matches = new List<DeclaredType>();
        for (var index = 0; index < declaredTypes.Count; index++)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var type = declaredTypes[index];
            if (!includeInterfaces && type.IsInterface)
                continue;

            var simpleName = type.FullName[(type.NamespaceName.Length + 1)..];
            if (!simpleName.EndsWith(suffix, StringComparison.Ordinal))
                continue;

            if (IsExcludedFullName(type.FullName, excludeFullNames))
                continue;

            matches.Add(type);
        }

        return matches;
    }

    /// <summary>Asserts every type resides in one of the given exact namespaces.</summary>
    /// <param name="types">Non-empty set of types under test.</param>
    /// <param name="exactNamespaces">Allowed exact namespace names.</param>
    /// <exception cref="ArgumentNullException"><paramref name="types" /> or <paramref name="exactNamespaces" /> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="types" /> or <paramref name="exactNamespaces" /> is empty.</exception>
    internal static void AssertResideInOneOfNamespaces(IReadOnlyList<DeclaredType> types, string[] exactNamespaces)
    {
        ArgumentNullException.ThrowIfNull(types);
        ArgumentNullException.ThrowIfNull(exactNamespaces);
        if (types.Count is 0)
            throw new ArgumentException("At least one discovered type is required.", nameof(types));
        if (exactNamespaces.Length is 0)
            throw new ArgumentException("At least one namespace is required.", nameof(exactNamespaces));

        for (var index = 0; index < types.Count; index++)
        {
            var type = types[index];
            Assert.True(
                ResidesInOneOfExactNamespaces(type.NamespaceName, exactNamespaces),
                $"Type '{type.FullName}' resides in '{type.NamespaceName}', which is not an approved namespace.");
        }
    }

    /// <summary>Asserts every type in a non-empty collection resides in the given exact namespace.</summary>
    /// <param name="types">Non-empty set of types under test.</param>
    /// <param name="exactNamespace">Required exact namespace.</param>
    /// <exception cref="ArgumentNullException"><paramref name="types" /> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="types" /> is empty or <paramref name="exactNamespace" /> is null or empty.</exception>
    internal static void AssertResideInNamespace(IReadOnlyList<DeclaredType> types, string exactNamespace)
    {
        ArgumentNullException.ThrowIfNull(types);
        ArgumentException.ThrowIfNullOrEmpty(exactNamespace);
        if (types.Count is 0)
            throw new ArgumentException("At least one discovered type is required.", nameof(types));

        for (var index = 0; index < types.Count; index++)
        {
            var type = types[index];
            Assert.True(
                string.Equals(type.NamespaceName, exactNamespace, StringComparison.Ordinal),
                $"Type '{type.FullName}' resides in '{type.NamespaceName}', expected '{exactNamespace}'.");
        }
    }

    private static Task<IReadOnlyList<DeclaredType>> AllDeclaredTypesAsync()
    {
        lock (ScanGate)
        {
            if (_allDeclaredTypesTask is { } existing)
                return existing;

            var task = ScanAsync();
            _allDeclaredTypesTask = task;
            const TaskContinuationOptions faultResetOptions = TaskContinuationOptions.NotOnRanToCompletion
                | TaskContinuationOptions.ExecuteSynchronously
                | TaskContinuationOptions.DenyChildAttach;
            _ = task.ContinueWith(
                static completed =>
                {
                    lock (ScanGate)
                    {
                        if (ReferenceEquals(_allDeclaredTypesTask, completed))
                            _allDeclaredTypesTask = null;
                    }
                },
                CancellationToken.None,
                faultResetOptions,
                TaskScheduler.Default);
            return task;
        }
    }

    private static async Task<IReadOnlyList<DeclaredType>> ScanAsync()
    {
        var matches = new List<DeclaredType>();
        var paths = ServerSourceFiles.EnumerateCsharpFiles();
        for (var pathIndex = 0; pathIndex < paths.Count; pathIndex++)
        {
            var lines = await File.ReadAllLinesAsync(paths[pathIndex], CancellationToken.None).ConfigureAwait(false);
            string? currentNamespace = null;
            for (var lineIndex = 0; lineIndex < lines.Length; lineIndex++)
            {
                var trimmed = lines[lineIndex].TrimStart();
                if (trimmed.StartsWith("namespace ", StringComparison.Ordinal))
                {
                    currentNamespace = ParseNamespace(trimmed);
                    continue;
                }

                if (currentNamespace is null || !IsUnderServerRoot(currentNamespace))
                    continue;

                if (!TryParseTypeName(trimmed, out var typeName, out var isInterface))
                    continue;

                matches.Add(new DeclaredType(currentNamespace + "." + typeName, currentNamespace, isInterface));
            }
        }

        return matches;
    }

    private static bool IsUnderServerRoot(string namespaceName)
    {
        const string root = ServerArchitectureNamespaces.Root;
        return string.Equals(namespaceName, root, StringComparison.Ordinal) ||
               namespaceName.StartsWith(root + ".", StringComparison.Ordinal);
    }

    private static bool IsExcludedFullName(string fullName, string[] excludeFullNames)
    {
        for (var index = 0; index < excludeFullNames.Length; index++)
        {
            if (string.Equals(fullName, excludeFullNames[index], StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static string ParseNamespace(string trimmedNamespaceLine)
    {
        var value = trimmedNamespaceLine["namespace ".Length..].Trim();
        var comment = value.IndexOf("//", StringComparison.Ordinal);
        if (comment >= 0)
            value = value[..comment].TrimEnd();

        if (value.Length > 0 && value[^1] is ';' or '{')
            value = value[..^1].TrimEnd();
        return value;
    }

    private static bool ResidesInOneOfExactNamespaces(string typeNamespace, string[] exactNamespaces)
    {
        for (var namespaceIndex = 0; namespaceIndex < exactNamespaces.Length; namespaceIndex++)
        {
            if (string.Equals(typeNamespace, exactNamespaces[namespaceIndex], StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static bool TryParseTypeName(string trimmed, out string typeName, out bool isInterface)
    {
        typeName = string.Empty;
        isInterface = false;
        var remaining = trimmed.AsSpan();

        while (true)
        {
            var probe = remaining;
            if (!TryReadToken(ref probe, out var token))
                return false;

            if (!DeclarationModifierLookup.Contains(token))
                break;

            remaining = probe;
        }

        if (!TryReadToken(ref remaining, out var kindSpan))
            return false;

        switch (kindSpan.ToString())
        {
            case "interface":
                isInterface = true;
                break;

            case "class":
            case "struct":
                break;

            case "record":
                var afterRecord = remaining;
                if (TryReadToken(ref afterRecord, out var optionalRecordKind))
                {
                    var optionalKind = optionalRecordKind.ToString();
                    if (string.Equals(optionalKind, "struct", StringComparison.Ordinal) || string.Equals(optionalKind, "class", StringComparison.Ordinal))
                        remaining = afterRecord;
                }

                break;

            default:
                return false;
        }

        typeName = ReadIdentifier(remaining.TrimStart());
        return typeName.Length > 0;
    }

    private static bool TryReadToken(ref ReadOnlySpan<char> text, out ReadOnlySpan<char> token)
    {
        text = text.TrimStart();
        if (text.IsEmpty)
        {
            token = default;
            return false;
        }

        var length = 0;
        while (length < text.Length && !char.IsWhiteSpace(text[length]))
            length++;

        token = text[..length];
        text = text[length..];
        return true;
    }

    private static string ReadIdentifier(ReadOnlySpan<char> text)
    {
        var length = 0;
        while (length < text.Length)
        {
            var ch = text[length];
            if (!(char.IsLetterOrDigit(ch) || ch is '_' or '@'))
                break;
            length++;
        }

        return length is 0 ? string.Empty : text[..length].ToString();
    }

    /// <summary>A type declaration discovered in server sources.</summary>
    /// <param name="FullName">Namespace-qualified type name.</param>
    /// <param name="NamespaceName">Containing namespace.</param>
    /// <param name="IsInterface"><see langword="true" /> when the declaration is an interface.</param>
    internal readonly record struct DeclaredType(string FullName, string NamespaceName, bool IsInterface);
}
