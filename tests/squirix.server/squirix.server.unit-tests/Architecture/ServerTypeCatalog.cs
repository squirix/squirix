using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Source-scan helpers for <c>Squirix.Server</c> type placement architecture rules.</summary>
internal static class ServerTypeCatalog
{
    private static readonly string[] TypeDeclarationPrefixes =
    [
        "public sealed class ",
        "internal sealed class ",
        "public sealed record ",
        "internal sealed record ",
        "public static class ",
        "internal static class ",
        "public class ",
        "internal class ",
        "public record ",
        "internal record ",
        "public struct ",
        "internal struct ",
        "public readonly struct ",
        "internal readonly struct ",
        "private sealed class ",
        "private static class ",
        "private class ",
        "file sealed class ",
        "file static class ",
        "file class ",
    ];

    private static readonly string[] InterfaceDeclarationPrefixes =
    [
        "public interface ",
        "internal interface ",
        "file interface ",
    ];

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

        var serverRoot = Path.Join(RepositoryPaths.FindRepositoryRoot(), "src", "squirix.server");
        Assert.True(Directory.Exists(serverRoot), $"Expected source root '{serverRoot}'.");

        var matches = new List<DeclaredType>();
        var objMarker = $"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}";
        foreach (var path in Directory.GetFiles(serverRoot, "*.cs", SearchOption.AllDirectories))
        {
            if (path.Contains(objMarker, StringComparison.Ordinal))
                continue;

            var lines = await File.ReadAllLinesAsync(path, cancellationToken);
            string? currentNamespace = null;
            for (var lineIndex = 0; lineIndex < lines.Length; lineIndex++)
            {
                var trimmed = lines[lineIndex].TrimStart();
                if (trimmed.StartsWith("namespace ", StringComparison.Ordinal))
                {
                    currentNamespace = ParseNamespace(trimmed);
                    continue;
                }

                if (currentNamespace?.Contains(ServerArchitectureNamespaces.Root, StringComparison.Ordinal) is not true)
                    continue;

                if (!TryParseTypeName(trimmed, includeInterfaces, out var typeName))
                    continue;

                if (!typeName.EndsWith(suffix, StringComparison.Ordinal))
                    continue;

                var fullName = currentNamespace + "." + typeName;
                if (IsExcludedFullName(fullName, excludeFullNames))
                    continue;

                matches.Add(new DeclaredType(fullName, currentNamespace));
            }
        }

        return matches;
    }

    /// <summary>Asserts every type resides in one of the given exact namespaces.</summary>
    /// <param name="types">Types under test.</param>
    /// <param name="exactNamespaces">Allowed exact namespace names.</param>
    /// <exception cref="ArgumentNullException"><paramref name="types" /> or <paramref name="exactNamespaces" /> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="exactNamespaces" /> is empty.</exception>
    internal static void AssertResideInOneOfNamespaces(IReadOnlyList<DeclaredType> types, string[] exactNamespaces)
    {
        ArgumentNullException.ThrowIfNull(types);
        ArgumentNullException.ThrowIfNull(exactNamespaces);
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

    /// <summary>Asserts every type resides in the given exact namespace. Empty input succeeds.</summary>
    /// <param name="types">Types under test.</param>
    /// <param name="exactNamespace">Required exact namespace.</param>
    internal static void AssertResideInNamespace(IReadOnlyList<DeclaredType> types, string exactNamespace)
    {
        ArgumentNullException.ThrowIfNull(types);
        ArgumentException.ThrowIfNullOrEmpty(exactNamespace);

        for (var index = 0; index < types.Count; index++)
        {
            var type = types[index];
            Assert.True(
                string.Equals(type.NamespaceName, exactNamespace, StringComparison.Ordinal),
                $"Type '{type.FullName}' resides in '{type.NamespaceName}', expected '{exactNamespace}'.");
        }
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

    private static bool TryParseTypeName(string trimmed, bool includeInterfaces, out string typeName)
    {
        typeName = string.Empty;

        for (var index = 0; index < TypeDeclarationPrefixes.Length; index++)
        {
            var prefix = TypeDeclarationPrefixes[index];
            if (!trimmed.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            typeName = ReadIdentifier(trimmed.AsSpan(prefix.Length));
            return typeName.Length > 0;
        }

        if (!includeInterfaces)
            return false;

        for (var index = 0; index < InterfaceDeclarationPrefixes.Length; index++)
        {
            var prefix = InterfaceDeclarationPrefixes[index];
            if (!trimmed.StartsWith(prefix, StringComparison.Ordinal))
                continue;

            typeName = ReadIdentifier(trimmed.AsSpan(prefix.Length));
            return typeName.Length > 0;
        }

        return false;
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
    internal readonly record struct DeclaredType(string FullName, string NamespaceName);
}
