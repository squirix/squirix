using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Globalization;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Squirix.TestKit.Testing;

/// <summary>Builds stable exported public API identity strings from assembly metadata via Roslyn symbols.</summary>
public static class ExportedApiMetadata
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

    private static void AddMemberIdentities(INamedTypeSymbol type, string typeIdentity, HashSet<string> identities)
    {
        var members = type.GetMembers();
        var isEnum = type.TypeKind is TypeKind.Enum;
        for (var i = 0; i < members.Length; i++)
        {
            var member = members[i];
            if (member.DeclaredAccessibility is not Accessibility.Public)
                continue;

            switch (member)
            {
                case IMethodSymbol { MethodKind: MethodKind.Constructor } constructor:
                    _ = identities.Add(FormatMethodLine(typeIdentity, constructor));
                    break;
                case IMethodSymbol method when IsOrdinaryMethod(method):
                    _ = identities.Add(FormatMethodLine(typeIdentity, method));
                    break;
                case IPropertySymbol property:
                    AddPropertyIdentities(typeIdentity, property, identities);
                    break;
                case IEventSymbol evt:
                    _ = identities.Add(FormatEventLine(typeIdentity, evt.Name));
                    break;
                case IFieldSymbol { IsImplicitlyDeclared: false, Name: not "value__" } field:
                    if (isEnum && !field.IsStatic)
                        break;

                    _ = identities.Add(FormatFieldLine(typeIdentity, field.Name));
                    break;
            }
        }
    }

    private static void AddPropertyIdentities(string typeIdentity, IPropertySymbol property, HashSet<string> identities)
    {
        var indexParameters = property.Parameters;
        if (indexParameters.Length > 0)
        {
            var indexParts = new string[indexParameters.Length];
            for (var i = 0; i < indexParameters.Length; i++)
                indexParts[i] = FormatTypeName(indexParameters[i].Type);

            var indexSignature = string.Join(',', indexParts);
            var propertyType = FormatTypeName(property.Type);
            if (property.GetMethod is { DeclaredAccessibility: Accessibility.Public })
                _ = identities.Add("P:" + typeIdentity + "::this[" + indexSignature + "]:" + propertyType + ".get");

            if (property.SetMethod is { DeclaredAccessibility: Accessibility.Public })
                _ = identities.Add("P:" + typeIdentity + "::this[" + indexSignature + "]:" + propertyType + ".set");

            return;
        }

        var typeName = FormatTypeName(property.Type);
        if (property.GetMethod is { DeclaredAccessibility: Accessibility.Public })
            _ = identities.Add("P:" + typeIdentity + "::" + property.Name + ":" + typeName + ".get");

        if (property.SetMethod is { DeclaredAccessibility: Accessibility.Public })
            _ = identities.Add("P:" + typeIdentity + "::" + property.Name + ":" + typeName + ".set");
    }

    private static void AddTypeIdentities(INamedTypeSymbol type, HashSet<string> identities)
    {
        var typeIdentity = FormatTypeIdentity(type);
        _ = identities.Add("T:" + typeIdentity);
        AddMemberIdentities(type, typeIdentity, identities);
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
            }
        }
    }

    private static void CollectExportedTypeTree(INamedTypeSymbol type, HashSet<string> identities)
    {
        if (!IsExportedPublicType(type))
            return;

        AddTypeIdentities(type, identities);
        var nestedTypes = type.GetTypeMembers();
        for (var i = 0; i < nestedTypes.Length; i++)
            CollectExportedTypeTree(nestedTypes[i], identities);
    }

    private static string FormatEventLine(string typeIdentity, string name) => "E:" + typeIdentity + "::" + name;

    private static string FormatFieldLine(string typeIdentity, string name) => "F:" + typeIdentity + "::" + name;

    private static string FormatMethodLine(string typeIdentity, IMethodSymbol method)
    {
        var name = method.MethodKind is MethodKind.Constructor ? ".ctor" : method.Name;
        return "M:" + typeIdentity + "::" + name + FormatParameterList(method.Parameters);
    }

    private static string FormatParameterList(ImmutableArray<IParameterSymbol> parameters)
    {
        if (parameters.IsDefaultOrEmpty)
            return "()";

        var parts = new string[parameters.Length];
        for (var i = 0; i < parameters.Length; i++)
            parts[i] = FormatParameterTypeName(parameters[i]);

        return "(" + string.Join(',', parts) + ")";
    }

    private static string FormatParameterTypeName(IParameterSymbol parameter)
    {
        var typeName = FormatTypeName(parameter.Type);
        return parameter.RefKind is RefKind.Ref or RefKind.Out or RefKind.In
            ? typeName + "&"
            : typeName;
    }

    private static string FormatTypeIdentity(INamedTypeSymbol type)
    {
        var definition = type.IsGenericType ? type.OriginalDefinition : type;
        return GetTypeMetadataName(definition);
    }

    private static string FormatTypeName(ITypeSymbol type)
    {
        switch (type)
        {
            case ITypeParameterSymbol typeParameter:
                return typeParameter.TypeParameterKind is TypeParameterKind.Method
                    ? "!" + typeParameter.Ordinal.ToString(CultureInfo.InvariantCulture)
                    : "!!" + typeParameter.Ordinal.ToString(CultureInfo.InvariantCulture);
            case IPointerTypeSymbol pointer:
                return FormatTypeName(pointer.PointedAtType) + "*";
            case IArrayTypeSymbol array:
                return FormatTypeName(array.ElementType) + "[" + new string(',', array.Rank - 1) + "]";
        }

        if (type is not INamedTypeSymbol { IsGenericType: true } namedType)
            return GetTypeMetadataName(type);
        var genericDefinitionName = GetTypeMetadataName(namedType.OriginalDefinition);
        var tick = genericDefinitionName.IndexOf('`', StringComparison.Ordinal);
        if (tick >= 0)
            genericDefinitionName = genericDefinitionName[..tick];

        var genericArguments = namedType.TypeArguments;
        var argumentNames = new string[genericArguments.Length];
        for (var i = 0; i < genericArguments.Length; i++)
            argumentNames[i] = FormatTypeName(genericArguments[i]);

        return genericDefinitionName + "<" + string.Join(',', argumentNames) + ">";
    }

    private static string GetTypeMetadataName(ITypeSymbol type)
    {
        if (TryGetSpecialTypeMetadataName(type.SpecialType) is { } specialTypeName)
            return specialTypeName;

        if (type is INamedTypeSymbol { IsGenericType: true } namedType && type.IsDefinition)
        {
            var ns = GetNamespace(type);
            var metadataName = namedType.MetadataName;
            return string.IsNullOrEmpty(ns) ? metadataName : ns + "." + metadataName;
        }

        var fullName = type.ToDisplayString(SymbolDisplayFormat.FullyQualifiedFormat);
        if (fullName.StartsWith("global::", StringComparison.Ordinal))
            fullName = fullName["global::".Length..];

        return fullName.Replace('+', '.');
    }

    private static string? TryGetSpecialTypeMetadataName(SpecialType specialType) =>
        specialType switch
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

    private static string GetNamespace(ITypeSymbol type)
    {
        var ns = type.ContainingNamespace;
        if (ns is null or { IsGlobalNamespace: true })
            return string.Empty;

        return ns.ToDisplayString();
    }

    private static bool IsExportedPublicType(INamedTypeSymbol type)
    {
        if (type.DeclaredAccessibility is not Accessibility.Public)
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

    private static bool IsOrdinaryMethod(IMethodSymbol method)
    {
        if (method.MethodKind is MethodKind.Constructor)
            return true;

        if (method.Name.StartsWith("op_", StringComparison.Ordinal))
            return true;

        return method.MethodKind is MethodKind.Ordinary;
    }
}
