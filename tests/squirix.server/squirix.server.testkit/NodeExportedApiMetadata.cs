using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Squirix.Server.TestKit;

/// <summary>Builds stable exported public API identity strings from assembly metadata via Roslyn symbols.</summary>
public static class NodeExportedApiMetadata
{
    /// <summary>Builds the set of stable exported public API identity strings used by broad public API snapshot tests.</summary>
    /// <param name="assemblyPath">Absolute path to the assembly under test.</param>
    /// <returns>Normalized type and member identities, compared with <see cref="StringComparer.Ordinal" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when Roslyn cannot load symbols for <paramref name="assemblyPath" />.</exception>
    public static HashSet<string> GetExportedApiIdentitySet(string assemblyPath)
    {
        ArgumentException.ThrowIfNullOrEmpty(assemblyPath);
        var reference = MetadataReference.CreateFromFile(assemblyPath);
        var compilation = CSharpCompilation.Create("ExportedApiSnapshot", references: [reference]);
        if (compilation.GetAssemblyOrModuleSymbol(reference) is not IAssemblySymbol assemblySymbol)
            throw new InvalidOperationException($"Could not load symbols from '{assemblyPath}'.");

        var identities = new HashSet<string>(StringComparer.Ordinal);
        CollectExportedTypes(assemblySymbol.GlobalNamespace, identities);
        return identities;
    }

    private static void AddFieldIdentity(string typeIdentity, IFieldSymbol field, bool isEnum, HashSet<string> identities)
    {
        if (field.IsImplicitlyDeclared || string.Equals(field.Name, "value__", StringComparison.Ordinal))
            return;

        if (isEnum && !field.IsStatic)
            return;

        _ = identities.Add(ApiIdentityFormatting.FormatFieldLine(typeIdentity, field.Name));
    }

    private static void AddIndexerIdentities(string typeIdentity, IPropertySymbol property, HashSet<string> identities)
    {
        var indexParameters = property.Parameters;
        var indexParts = new string[indexParameters.Length];
        for (var i = 0; i < indexParameters.Length; i++)
            indexParts[i] = ApiIdentityFormatting.FormatTypeName(indexParameters[i].Type);

        var indexSignature = string.Join(',', indexParts);
        var propertyType = ApiIdentityFormatting.FormatTypeName(property.Type);
        if (property.GetMethod is { DeclaredAccessibility: Accessibility.Public })
            _ = identities.Add($"P:{typeIdentity}::this[{indexSignature}]:{propertyType}.get");

        if (property.SetMethod is { DeclaredAccessibility: Accessibility.Public })
            _ = identities.Add($"P:{typeIdentity}::this[{indexSignature}]:{propertyType}.set");
    }

    private static void AddMemberIdentities(INamedTypeSymbol type, string typeIdentity, HashSet<string> identities)
    {
        var members = type.GetMembers();
        var isEnum = type.TypeKind is TypeKind.Enum;
        for (var i = 0; i < members.Length; i++)
            AddPublicMemberIdentity(members[i], typeIdentity, isEnum, identities);
    }

    private static void AddMethodIdentity(string typeIdentity, IMethodSymbol method, HashSet<string> identities)
    {
        if (method.MethodKind is MethodKind.Constructor || ApiIdentityFormatting.IsOrdinaryMethod(method))
            _ = identities.Add(ApiIdentityFormatting.FormatMethodLine(typeIdentity, method));
    }

    private static void AddPropertyIdentities(string typeIdentity, IPropertySymbol property, HashSet<string> identities)
    {
        if (property.Parameters.Length > 0)
        {
            AddIndexerIdentities(typeIdentity, property, identities);
            return;
        }

        var typeName = ApiIdentityFormatting.FormatTypeName(property.Type);
        if (property.GetMethod is { DeclaredAccessibility: Accessibility.Public })
            _ = identities.Add($"P:{typeIdentity}::{property.Name}:{typeName}.get");

        if (property.SetMethod is { DeclaredAccessibility: Accessibility.Public })
            _ = identities.Add($"P:{typeIdentity}::{property.Name}:{typeName}.set");
    }

    private static void AddPublicMemberIdentity(ISymbol member, string typeIdentity, bool isEnum, HashSet<string> identities)
    {
        if (member.DeclaredAccessibility != Accessibility.Public)
            return;

        if (member is IMethodSymbol method)
        {
            AddMethodIdentity(typeIdentity, method, identities);
            return;
        }

        if (member is IPropertySymbol property)
        {
            AddPropertyIdentities(typeIdentity, property, identities);
            return;
        }

        if (member is IEventSymbol)
        {
            _ = identities.Add(ApiIdentityFormatting.FormatEventLine(typeIdentity, member.Name));
            return;
        }

        if (member is IFieldSymbol field)
            AddFieldIdentity(typeIdentity, field, isEnum, identities);
    }

    private static void AddTypeIdentities(INamedTypeSymbol type, HashSet<string> identities)
    {
        var typeIdentity = ApiIdentityFormatting.FormatTypeIdentity(type);
        _ = identities.Add($"T:{typeIdentity}");
        AddMemberIdentities(type, typeIdentity, identities);
    }

    private static void CollectExportedTypeTree(INamedTypeSymbol type, HashSet<string> identities)
    {
        if (!ApiIdentityFormatting.IsExportedPublicType(type))
            return;

        AddTypeIdentities(type, identities);
        var nestedTypes = type.GetTypeMembers();
        for (var i = 0; i < nestedTypes.Length; i++)
            CollectExportedTypeTree(nestedTypes[i], identities);
    }

    private static void CollectExportedTypes(INamespaceSymbol namespaceSymbol, HashSet<string> identities)
    {
        foreach (var member in namespaceSymbol.GetMembers())
        {
            switch (member)
            {
                case INamespaceSymbol nestedNamespace:
                    CollectExportedTypes(nestedNamespace, identities);
                    break;
                case INamedTypeSymbol type:
                    CollectExportedTypeTree(type, identities);
                    break;
                default:
                    throw new InvalidOperationException("Encountered unexpected member.");
            }
        }
    }

    /// <summary>Formatting and naming helpers for exported API identity strings.</summary>
    private static class ApiIdentityFormatting
    {
        internal static string FormatEventLine(string typeIdentity, string name) => $"E:{typeIdentity}::{name}";

        internal static string FormatFieldLine(string typeIdentity, string name) => $"F:{typeIdentity}::{name}";

        internal static string FormatGenericTypeName(INamedTypeSymbol namedType)
        {
            var genericDefinitionName = GetTypeMetadataName(namedType.OriginalDefinition);
            var tick = genericDefinitionName.IndexOf('`', StringComparison.Ordinal);
            if (tick >= 0)
                genericDefinitionName = genericDefinitionName[..tick];

            var genericArguments = namedType.TypeArguments;
            var argumentNames = new string[genericArguments.Length];
            for (var i = 0; i < genericArguments.Length; i++)
                argumentNames[i] = FormatTypeName(genericArguments[i]);

            return $"{genericDefinitionName}<{string.Join(',', argumentNames)}>";
        }

        internal static string FormatMethodLine(string typeIdentity, IMethodSymbol method)
        {
            var name = method.MethodKind is MethodKind.Constructor ? ".ctor" : method.Name;
            return $"M:{typeIdentity}::{name}{FormatParameterList(method.Parameters)}";
        }

        internal static string FormatParameterList(ImmutableArray<IParameterSymbol> parameters)
        {
            if (parameters.IsDefaultOrEmpty)
                return "()";

            var parts = new string[parameters.Length];
            for (var i = 0; i < parameters.Length; i++)
                parts[i] = FormatParameterTypeName(parameters[i]);

            return $"({string.Join(',', parts)})";
        }

        internal static string FormatParameterTypeName(IParameterSymbol parameter)
        {
            var typeName = FormatTypeName(parameter.Type);
            return parameter.RefKind is RefKind.Ref or RefKind.Out or RefKind.In ? $"{typeName}&" : typeName;
        }

        internal static string FormatTypeIdentity(INamedTypeSymbol type)
        {
            var definition = type.IsGenericType ? type.OriginalDefinition : type;
            return GetTypeMetadataName(definition);
        }

        internal static string FormatTypeName(ITypeSymbol type)
        {
            return type switch
            {
                ITypeParameterSymbol typeParameter => FormatTypeParameterName(typeParameter),
                IPointerTypeSymbol pointer => $"{FormatTypeName(pointer.PointedAtType)}*",
                IArrayTypeSymbol array => array.Rank == 1 ? $"{FormatTypeName(array.ElementType)}[]" : $"{FormatTypeName(array.ElementType)}[{new string(',', array.Rank - 1)}]",
                _ => type is INamedTypeSymbol { IsGenericType: true } namedType ? FormatGenericTypeName(namedType) : GetTypeMetadataName(type),
            };
        }

        internal static string FormatTypeParameterName(ITypeParameterSymbol typeParameter) => typeParameter.TypeParameterKind is TypeParameterKind.Method
            ? $"!{NodeInvariantIndexStrings.Format(typeParameter.Ordinal)}" : $"!!{NodeInvariantIndexStrings.Format(typeParameter.Ordinal)}";

        internal static string GetNamespace(ITypeSymbol type)
        {
            var ns = type.ContainingNamespace;
            if (ns is null or { IsGlobalNamespace: true })
                return string.Empty;

            return ns.ToDisplayString();
        }

        internal static string? GetSpecialTypeMetadataName(SpecialType specialType) => specialType switch
        {
            SpecialType.System_Boolean => "System.Boolean",
            SpecialType.System_Byte => "System.Byte",
            SpecialType.System_SByte => "System.SByte",
            SpecialType.System_Char => "System.Char",
            SpecialType.System_Int16 => "System.Int16",
            SpecialType.System_Int32 => "System.Int32",
            SpecialType.System_Int64 => "System.Int64",
            SpecialType.System_UInt16 => "System.UInt16",
            SpecialType.System_UInt32 => "System.UInt32",
            SpecialType.System_UInt64 => "System.UInt64",
            SpecialType.System_Single => "System.Single",
            SpecialType.System_Double => "System.Double",
            SpecialType.System_Decimal => "System.Decimal",
            SpecialType.System_String => "System.String",
            SpecialType.System_Object => "System.Object",
            SpecialType.System_Void => "System.Void",
            SpecialType.System_DateTime => "System.DateTime",
            _ => null,
        };

        internal static string GetTypeMetadataName(ITypeSymbol type)
        {
            if (GetSpecialTypeMetadataName(type.SpecialType) is { } specialTypeName)
                return specialTypeName;

            if (type is INamedTypeSymbol { IsGenericType: true } namedType && type.IsDefinition)
            {
                var ns = GetNamespace(type);
                var metadataName = namedType.MetadataName;
                return string.IsNullOrEmpty(ns) ? metadataName : $"{ns}.{metadataName}";
            }

            var fullName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
            if (fullName.StartsWith("global::", StringComparison.Ordinal))
                fullName = fullName["global::".Length..];

            return fullName.Replace('+', '.');
        }

        internal static bool IsExportedPublicType(INamedTypeSymbol type)
        {
            if (type.DeclaredAccessibility != Accessibility.Public)
                return false;

            if (type.Name.Contains('<', StringComparison.Ordinal))
                return false;

            var attributes = type.GetAttributes();
            for (var i = 0; i < attributes.Length; i++)
            {
                if (string.Equals(attributes[i].AttributeClass?.Name, "CompilerGeneratedAttribute", StringComparison.Ordinal))
                    return false;
            }

            return true;
        }

        internal static bool IsOrdinaryMethod(IMethodSymbol method)
        {
            if (method.MethodKind is MethodKind.Constructor)
                return true;

            if (method.Name.StartsWith("op_", StringComparison.Ordinal))
                return true;

            return method.MethodKind is MethodKind.Ordinary;
        }
    }
}
