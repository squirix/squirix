using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Squirix.TestKit;

/// <summary>
/// Helpers for reflection-based public exported type analysis in tests.
/// </summary>
public static class ExportedTypeReflection
{
    private const BindingFlags DeclaredMemberFlags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    /// <summary>
    /// Builds the set of stable exported public API identity strings used by broad public API snapshot tests.
    /// </summary>
    /// <param name="assembly">Assembly whose exported API is summarized.</param>
    /// <returns>Normalized type and member identities, compared with <see cref="StringComparer.Ordinal" />.</returns>
    public static HashSet<string> GetExportedApiIdentitySet(Assembly assembly) => GetExportedApiIdentities(assembly).ToHashSet(StringComparer.Ordinal);

    /// <summary>
    /// Yields exported types from <paramref name="assembly" />, excluding compiler-generated artifacts.
    /// </summary>
    /// <param name="assembly">Assembly to enumerate.</param>
    /// <returns>Filtered exported types.</returns>
    private static IEnumerable<Type> EnumerateExportedTypesExcludingCompilerArtifacts(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        return assembly.GetExportedTypes().Where(static t => !IsCompilerGeneratedPublicArtifact(t));
    }

    private static string FormatEventLine(EventInfo evt) => "E:" + FormatTypeIdentity(evt.DeclaringType!) + "::" + evt.Name;

    private static string FormatFieldLine(FieldInfo field) => "F:" + FormatTypeIdentity(field.DeclaringType!) + "::" + field.Name;

    private static string FormatMethodLine(MethodBase method)
    {
        var name = method is ConstructorInfo ? ".ctor" : method.Name;
        return "M:" + FormatTypeIdentity(method.DeclaringType!) + "::" + name + FormatParameterList(method.GetParameters());
    }

    private static string FormatParameterList(ParameterInfo[] parameters) =>
        "(" + string.Join(",", parameters.Select(static parameter => FormatTypeName(parameter.ParameterType))) + ")";

    private static IEnumerable<string> FormatPropertyLines(PropertyInfo property)
    {
        var declaring = FormatTypeIdentity(property.DeclaringType!);
        var indexParameters = property.GetIndexParameters();
        if (indexParameters.Length > 0)
        {
            var indexSignature = string.Join(",", indexParameters.Select(static parameter => FormatTypeName(parameter.ParameterType)));
            if (property.GetMethod?.IsPublic == true)
                yield return "P:" + declaring + "::this[" + indexSignature + "].get";

            if (property.SetMethod?.IsPublic == true)
                yield return "P:" + declaring + "::this[" + indexSignature + "].set";

            yield break;
        }

        if (property.GetMethod?.IsPublic == true)
            yield return "P:" + declaring + "::" + property.Name + ".get";

        if (property.SetMethod?.IsPublic == true)
            yield return "P:" + declaring + "::" + property.Name + ".set";
    }

    private static string FormatTypeIdentity(Type type)
    {
        var definition = type.IsGenericType ? type.GetGenericTypeDefinition() : type;
        return definition.FullName ?? definition.Name;
    }

    private static string FormatTypeLine(Type type) => "T:" + FormatTypeIdentity(type);

    private static string FormatTypeName(Type type)
    {
        if (type.IsGenericTypeParameter)
            return "!!" + type.GenericParameterPosition.ToString(CultureInfo.InvariantCulture);

        if (type.IsGenericMethodParameter)
            return "!" + type.GenericParameterPosition.ToString(CultureInfo.InvariantCulture);

        if (type.IsByRef)
            return FormatTypeName(type.GetElementType()!) + "&";

        if (type.IsPointer)
            return FormatTypeName(type.GetElementType()!) + "*";

        if (type.IsArray)
            return FormatTypeName(type.GetElementType()!) + "[" + new string(',', type.GetArrayRank() - 1) + "]";

        if (!type.IsGenericType)
            return type.FullName ?? type.Name;

        var genericDefinition = type.GetGenericTypeDefinition();
        var genericDefinitionName = genericDefinition.FullName ?? genericDefinition.Name;
        var tick = genericDefinitionName.IndexOf('`', StringComparison.Ordinal);
        if (tick >= 0)
            genericDefinitionName = genericDefinitionName[..tick];

        return genericDefinitionName + "<" + string.Join(",", type.GetGenericArguments().Select(FormatTypeName)) + ">";
    }

    /// <summary>
    /// Builds stable exported public API identity strings, ordered by type and then by member.
    /// </summary>
    /// <param name="assembly">Assembly whose exported API is summarized.</param>
    /// <returns>Normalized type and member identities in snapshot order.</returns>
    private static List<string> GetExportedApiIdentities(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        var lines = new List<string>();
        foreach (var type in EnumerateExportedTypesExcludingCompilerArtifacts(assembly).OrderBy(static type => type.FullName, StringComparer.Ordinal))
        {
            lines.Add(FormatTypeLine(type));
            if (type.IsEnum)
            {
                foreach (var field in type.GetFields(DeclaredMemberFlags).Where(static field => field is { IsStatic: true, IsPublic: true })
                                          .OrderBy(FormatFieldLine, StringComparer.Ordinal))
                {
                    lines.Add(FormatFieldLine(field));
                }

                continue;
            }

            var memberLines = new List<string>();

            memberLines.AddRange(type.GetConstructors(DeclaredMemberFlags).Select(FormatMethodLine));
            memberLines.AddRange(type.GetMethods(DeclaredMemberFlags).Where(IsOrdinaryMethod).Select(FormatMethodLine));
            memberLines.AddRange(type.GetProperties(DeclaredMemberFlags).SelectMany(FormatPropertyLines));
            memberLines.AddRange(type.GetEvents(DeclaredMemberFlags).Select(FormatEventLine));
            memberLines.AddRange(type.GetFields(DeclaredMemberFlags).Where(static field => !field.IsSpecialName).Select(FormatFieldLine));

            lines.AddRange(memberLines.OrderBy(static line => line, StringComparer.Ordinal));
        }

        return lines;
    }

    /// <summary>
    /// Returns whether <paramref name="type" /> is a compiler-emitted public artifact
    /// (async state machine, display class, etc.) that public API snapshots should ignore.
    /// </summary>
    /// <param name="type">The CLR type to inspect.</param>
    /// <returns><see langword="true" /> when the type is attributed as compiler-generated or has a mangled name marker.</returns>
    private static bool IsCompilerGeneratedPublicArtifact(Type type)
    {
        ArgumentNullException.ThrowIfNull(type);
        return type.GetCustomAttribute<CompilerGeneratedAttribute>() is not null || type.FullName?.Contains('<', StringComparison.Ordinal) == true;
    }

    private static bool IsOrdinaryMethod(MethodInfo method) => !method.IsSpecialName || method.Name.StartsWith("op_", StringComparison.Ordinal);
}
