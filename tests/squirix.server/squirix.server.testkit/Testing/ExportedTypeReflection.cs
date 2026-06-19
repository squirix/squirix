using System;
using System.Collections.Generic;
using System.Globalization;
using System.Reflection;
using System.Runtime.CompilerServices;

namespace Squirix.Server.TestKit.Testing;

/// <summary>Helpers for reflection-based public exported type analysis in tests.</summary>
public static class ExportedTypeReflection
{
    private const BindingFlags DeclaredMemberFlags = BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static | BindingFlags.DeclaredOnly;

    /// <summary>Builds the set of stable exported public API identity strings used by broad public API snapshot tests.</summary>
    /// <param name="assembly">Assembly whose exported API is summarized.</param>
    /// <returns>Normalized type and member identities, compared with <see cref="StringComparer.Ordinal" />.</returns>
    public static IReadOnlySet<string> GetExportedApiIdentitySet(Assembly assembly) => new HashSet<string>(GetExportedApiIdentities(assembly), StringComparer.Ordinal);

    private static string FormatEventLine(EventInfo evt) => "E:" + FormatTypeIdentity(evt.DeclaringType!) + "::" + evt.Name;

    private static string FormatFieldLine(FieldInfo field) => "F:" + FormatTypeIdentity(field.DeclaringType!) + "::" + field.Name;

    private static string FormatMethodLine(MethodBase method)
    {
        var name = method is ConstructorInfo ? ".ctor" : method.Name;
        return "M:" + FormatTypeIdentity(method.DeclaringType!) + "::" + name + FormatParameterList(method.GetParameters());
    }

    private static string FormatParameterList(ParameterInfo[] parameters)
    {
        if (parameters.Length is 0)
            return "()";

        var parts = new string[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
            parts[i] = FormatTypeName(parameters[i].ParameterType);

        return "(" + string.Join(',', parts) + ")";
    }

    private static IEnumerable<string> FormatPropertyLines(PropertyInfo property)
    {
        var declaring = FormatTypeIdentity(property.DeclaringType!);
        var indexParameters = property.GetIndexParameters();
        if (indexParameters.Length > 0)
        {
            var indexParts = new string[indexParameters.Length];
            for (var i = 0; i < indexParameters.Length; i++)
                indexParts[i] = FormatTypeName(indexParameters[i].ParameterType);

            var indexSignature = string.Join(',', indexParts);
            var propertyType = FormatTypeName(property.PropertyType);
            if (property.GetMethod?.IsPublic is true)
                yield return "P:" + declaring + "::this[" + indexSignature + "]:" + propertyType + ".get";

            if (property.SetMethod?.IsPublic is true)
                yield return "P:" + declaring + "::this[" + indexSignature + "]:" + propertyType + ".set";

            yield break;
        }

        var typeName = FormatTypeName(property.PropertyType);
        if (property.GetMethod?.IsPublic is true)
            yield return "P:" + declaring + "::" + property.Name + ":" + typeName + ".get";

        if (property.SetMethod?.IsPublic is true)
            yield return "P:" + declaring + "::" + property.Name + ":" + typeName + ".set";
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

        var genericArguments = type.GetGenericArguments();
        var argumentNames = new string[genericArguments.Length];
        for (var i = 0; i < genericArguments.Length; i++)
            argumentNames[i] = FormatTypeName(genericArguments[i]);

        return genericDefinitionName + "<" + string.Join(',', argumentNames) + ">";
    }

    /// <summary>Builds stable exported public API identity strings, ordered by type and then by member.</summary>
    /// <param name="assembly">Assembly whose exported API is summarized.</param>
    /// <returns>Normalized type and member identities in snapshot order.</returns>
    private static List<string> GetExportedApiIdentities(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);

        var lines = new List<string>();
        var types = GetExportedTypesSorted(assembly);
        foreach (var type in types)
        {
            lines.Add(FormatTypeLine(type));
            if (type.IsEnum)
            {
                AddEnumFieldLines(type, lines);
                continue;
            }

            var memberLines = new List<string>();
            AddMemberLines(type, memberLines);
            memberLines.Sort(StringComparer.Ordinal);
            lines.AddRange(memberLines);
        }

        return lines;
    }

    private static void AddEnumFieldLines(Type type, List<string> lines)
    {
        var enumFields = new List<FieldInfo>();
        foreach (var field in type.GetFields(DeclaredMemberFlags))
        {
            if (field is { IsStatic: true, IsPublic: true })
                enumFields.Add(field);
        }

        enumFields.Sort(static (left, right) => StringComparer.Ordinal.Compare(FormatFieldLine(left), FormatFieldLine(right)));
        foreach (var field in enumFields)
            lines.Add(FormatFieldLine(field));
    }

    private static void AddMemberLines(Type type, List<string> memberLines)
    {
        foreach (var constructor in type.GetConstructors(DeclaredMemberFlags))
            memberLines.Add(FormatMethodLine(constructor));

        foreach (var method in type.GetMethods(DeclaredMemberFlags))
        {
            if (IsOrdinaryMethod(method))
                memberLines.Add(FormatMethodLine(method));
        }

        foreach (var property in type.GetProperties(DeclaredMemberFlags))
        {
            foreach (var line in FormatPropertyLines(property))
                memberLines.Add(line);
        }

        foreach (var evt in type.GetEvents(DeclaredMemberFlags))
            memberLines.Add(FormatEventLine(evt));

        foreach (var field in type.GetFields(DeclaredMemberFlags))
        {
            if (!field.IsSpecialName)
                memberLines.Add(FormatFieldLine(field));
        }
    }

    private static List<Type> GetExportedTypesSorted(Assembly assembly)
    {
        var types = new List<Type>();
        foreach (var type in assembly.GetExportedTypes())
        {
            if (!IsCompilerGeneratedPublicArtifact(type))
                types.Add(type);
        }

        types.Sort(static (left, right) => StringComparer.Ordinal.Compare(left.FullName, right.FullName));
        return types;
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
        return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute)) || type.FullName?.Contains('<', StringComparison.Ordinal) is true;
    }

    private static bool IsOrdinaryMethod(MethodInfo method) => !method.IsSpecialName || method.Name.StartsWith("op_", StringComparison.Ordinal);
}
