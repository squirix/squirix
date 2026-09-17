using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Runtime.InteropServices;

namespace Squirix.Server.TestKit;

/// <summary>Builds stable exported public API identity strings from assembly metadata via System.Reflection.Metadata.</summary>
public static class NodeExportedApiMetadata
{
    /// <summary>Builds the set of stable exported public API identity strings used by broad public API snapshot tests.</summary>
    /// <param name="assemblyPath">Absolute path to the assembly under test.</param>
    /// <returns>Normalized type and member identities, compared with <see cref="StringComparer.Ordinal" />.</returns>
    /// <exception cref="InvalidOperationException">Thrown when metadata cannot be loaded for <paramref name="assemblyPath" />.</exception>
    public static HashSet<string> GetExportedApiIdentitySet(string assemblyPath)
    {
        ArgumentException.ThrowIfNullOrEmpty(assemblyPath);
        try
        {
            var peImage = File.ReadAllBytes(assemblyPath);
            using var peReader = new PEReader(ImmutableCollectionsMarshal.AsImmutableArray(peImage));
            if (!peReader.HasMetadata)
                throw new InvalidOperationException($"Could not load symbols from '{assemblyPath}'.");

            var reader = peReader.GetMetadataReader();
            var identities = new HashSet<string>(StringComparer.Ordinal);
            var provider = new ApiSignatureTypeProvider();
            foreach (var typeHandle in reader.TypeDefinitions)
            {
                var typeDef = reader.GetTypeDefinition(typeHandle);
                if (!IsExportedPublicType(reader, typeDef))
                    continue;

                var typeIdentity = ApiSignatureTypeProvider.GetTypeIdentity(reader, typeDef);
                _ = identities.Add($"T:{typeIdentity}");
                AddMemberIdentities(reader, provider, typeDef, typeIdentity, identities);
            }

            return identities;
        }
        catch (InvalidOperationException)
        {
            throw;
        }
        catch (Exception ex) when (ex is IOException || ex is BadImageFormatException || ex is FileNotFoundException || ex is DirectoryNotFoundException ||
                                   ex is UnauthorizedAccessException)
        {
            throw new InvalidOperationException($"Could not load symbols from '{assemblyPath}'.", ex);
        }
    }

    private static void AddEventIdentities(MetadataReader reader, TypeDefinition typeDef, string typeIdentity, HashSet<string> identities)
    {
        foreach (var eventHandle in typeDef.GetEvents())
        {
            var eventDef = reader.GetEventDefinition(eventHandle);
            var name = reader.GetString(eventDef.Name);
            if (!IsPublicEvent(reader, eventDef))
                continue;

            _ = identities.Add($"E:{typeIdentity}::{name}");
        }
    }

    private static void AddFieldIdentities(MetadataReader reader, TypeDefinition typeDef, bool isEnum, string typeIdentity, HashSet<string> identities)
    {
        foreach (var fieldHandle in typeDef.GetFields())
        {
            var fieldDef = reader.GetFieldDefinition(fieldHandle);
            if ((fieldDef.Attributes & FieldAttributes.FieldAccessMask) != FieldAttributes.Public)
                continue;

            var name = reader.GetString(fieldDef.Name);
            if (string.Equals(name, "value__", StringComparison.Ordinal))
                continue;

            if (name.Contains('<', StringComparison.Ordinal))
                continue;

            if (isEnum && (fieldDef.Attributes & FieldAttributes.Static) != FieldAttributes.Static)
                continue;

            _ = identities.Add($"F:{typeIdentity}::{name}");
        }
    }

    private static void AddMemberIdentities(MetadataReader reader, ApiSignatureTypeProvider provider, TypeDefinition typeDef, string typeIdentity, HashSet<string> identities)
    {
        var isEnum = IsEnumType(reader, typeDef);
        AddMethodIdentities(reader, provider, typeDef, typeIdentity, identities);
        AddPropertyIdentities(reader, provider, typeDef, typeIdentity, identities);
        AddEventIdentities(reader, typeDef, typeIdentity, identities);
        AddFieldIdentities(reader, typeDef, isEnum, typeIdentity, identities);
    }

    private static void AddMethodIdentities(MetadataReader reader, ApiSignatureTypeProvider provider, TypeDefinition typeDef, string typeIdentity, HashSet<string> identities)
    {
        foreach (var methodHandle in typeDef.GetMethods())
        {
            if (FormatMethodIdentity(reader, provider, methodHandle, typeIdentity) is not { } identity)
                continue;

            _ = identities.Add(identity);
        }
    }

    private static void AddPropertyIdentities(MetadataReader reader, ApiSignatureTypeProvider provider, TypeDefinition typeDef, string typeIdentity, HashSet<string> identities)
    {
        foreach (var propertyHandle in typeDef.GetProperties())
        {
            var propertyDef = reader.GetPropertyDefinition(propertyHandle);
            var accessors = propertyDef.GetAccessors();
            var hasPublicGetter = IsPublicMethod(reader, accessors.Getter);
            var hasPublicSetter = IsPublicMethod(reader, accessors.Setter);
            if (!hasPublicGetter && !hasPublicSetter)
                continue;

            MethodSignature<string> signature;
            try
            {
                signature = propertyDef.DecodeSignature(provider, null);
            }
            catch (BadImageFormatException)
            {
                continue;
            }

            AddPropertyLines(propertyDef, reader, signature, typeIdentity, hasPublicGetter, hasPublicSetter, identities);
        }
    }

    private static void AddPropertyLines(
        PropertyDefinition propertyDef,
        MetadataReader reader,
        MethodSignature<string> signature,
        string typeIdentity,
        bool hasPublicGetter,
        bool hasPublicSetter,
        HashSet<string> identities)
    {
        var name = reader.GetString(propertyDef.Name);
        var propertyType = signature.ReturnType;
        var indexParameters = signature.ParameterTypes;
        if (indexParameters.Length > 0)
        {
            var indexSignature = ApiSignatureTypeProvider.FormatTypeList(indexParameters);
            if (hasPublicGetter)
                _ = identities.Add($"P:{typeIdentity}::this[{indexSignature}]:{propertyType}.get");

            if (hasPublicSetter)
                _ = identities.Add($"P:{typeIdentity}::this[{indexSignature}]:{propertyType}.set");
        }
        else
        {
            if (hasPublicGetter)
                _ = identities.Add($"P:{typeIdentity}::{name}:{propertyType}.get");

            if (hasPublicSetter)
                _ = identities.Add($"P:{typeIdentity}::{name}:{propertyType}.set");
        }
    }

    private static string? FormatMethodIdentity(MetadataReader reader, ApiSignatureTypeProvider provider, MethodDefinitionHandle methodHandle, string typeIdentity)
    {
        var methodDef = reader.GetMethodDefinition(methodHandle);
        if ((methodDef.Attributes & MethodAttributes.MemberAccessMask) != MethodAttributes.Public)
            return null;

        var name = reader.GetString(methodDef.Name);
        if (!IsIncludedMethod(methodDef, name, out var isCtor))
            return null;

        MethodSignature<string> signature;
        try
        {
            signature = methodDef.DecodeSignature(provider, null);
        }
        catch (BadImageFormatException)
        {
            return null;
        }

        var displayName = isCtor ? ".ctor" : name;
        var parameters = ApiSignatureTypeProvider.FormatTypeList(signature.ParameterTypes);
        return $"M:{typeIdentity}::{displayName}({parameters})";
    }

    private static bool HasCompilerGeneratedAttribute(MetadataReader reader, TypeDefinition typeDef)
    {
        foreach (var attributeHandle in typeDef.GetCustomAttributes())
        {
            var attribute = reader.GetCustomAttribute(attributeHandle);
            var ctor = attribute.Constructor;
            string? attributeName = null;
            if (ctor.Kind == HandleKind.MemberReference)
            {
                var memberRef = reader.GetMemberReference((MemberReferenceHandle)ctor);
                var parent = memberRef.Parent;
                if (parent.Kind == HandleKind.TypeReference)
                    attributeName = reader.GetString(reader.GetTypeReference((TypeReferenceHandle)parent).Name);
                else if (parent.Kind == HandleKind.TypeDefinition)
                    attributeName = reader.GetString(reader.GetTypeDefinition((TypeDefinitionHandle)parent).Name);
            }
            else if (ctor.Kind == HandleKind.MethodDefinition)
            {
                var methodDef = reader.GetMethodDefinition((MethodDefinitionHandle)ctor);
                attributeName = reader.GetString(reader.GetTypeDefinition(methodDef.GetDeclaringType()).Name);
            }

            if (string.Equals(attributeName, "CompilerGeneratedAttribute", StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static bool IsEnumType(MetadataReader reader, TypeDefinition typeDef)
    {
        var baseType = typeDef.BaseType;
        if (baseType.Kind != HandleKind.TypeReference)
            return false;

        var baseRef = reader.GetTypeReference((TypeReferenceHandle)baseType);
        return string.Equals(reader.GetString(baseRef.Namespace), "System", StringComparison.Ordinal) &&
               string.Equals(reader.GetString(baseRef.Name), "Enum", StringComparison.Ordinal);
    }

    private static bool IsExportedPublicType(MetadataReader reader, TypeDefinition typeDef)
    {
        var visibility = typeDef.Attributes & TypeAttributes.VisibilityMask;
        var declaring = typeDef.GetDeclaringType();
        var isPublic = declaring.IsNil ? visibility == TypeAttributes.Public : visibility == TypeAttributes.NestedPublic;
        if (!isPublic)
            return false;

        var name = reader.GetString(typeDef.Name);
        if (name.Contains('<', StringComparison.Ordinal))
            return false;

        if (HasCompilerGeneratedAttribute(reader, typeDef))
            return false;

        if (declaring.IsNil)
            return true;

        var parentDef = reader.GetTypeDefinition(declaring);
        return IsExportedPublicType(reader, parentDef);
    }

    private static bool IsIncludedMethod(MethodDefinition methodDef, string name, out bool isCtor)
    {
        isCtor = string.Equals(name, ".ctor", StringComparison.Ordinal);
        return isCtor || (!string.Equals(name, ".cctor", StringComparison.Ordinal) && (name.StartsWith("op_", StringComparison.Ordinal) ||
                                                                                       (methodDef.Attributes & (MethodAttributes.SpecialName | MethodAttributes.RTSpecialName)) ==
                                                                                       MethodAttributes.PrivateScope));
    }

    private static bool IsPublicEvent(MetadataReader reader, EventDefinition eventDef)
    {
        var accessors = eventDef.GetAccessors();
        return IsPublicMethod(reader, accessors.Adder) || IsPublicMethod(reader, accessors.Remover);
    }

    private static bool IsPublicMethod(MetadataReader reader, MethodDefinitionHandle handle)
    {
        if (handle.IsNil)
            return false;

        var methodDef = reader.GetMethodDefinition(handle);
        return (methodDef.Attributes & MethodAttributes.MemberAccessMask) == MethodAttributes.Public;
    }

    /// <summary>Decodes ECMA signatures into the golden identity type-name format.</summary>
    private sealed class ApiSignatureTypeProvider : ISignatureTypeProvider<string, object?>
    {
        public string GetArrayType(string elementType, ArrayShape shape) => shape.Rank == 1 ? $"{elementType}[]" : $"{elementType}[{new string(',', shape.Rank - 1)}]";

        public string GetByReferenceType(string elementType) => $"{elementType}&";

        public string GetFunctionPointerType(MethodSignature<string> signature) => $"fnptr<{signature.ReturnType}({FormatTypeList(signature.ParameterTypes)})>";

        public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments)
        {
            var tick = genericType.IndexOf('`', StringComparison.Ordinal);
            var definition = tick >= 0 ? genericType[..tick] : genericType;
            return $"{definition}<{FormatTypeList(typeArguments)}>";
        }

        public string GetGenericMethodParameter(object? genericContext, int index) => $"!{NodeInvariantIndexStrings.Format(index)}";

        public string GetGenericTypeParameter(object? genericContext, int index) => $"!!{NodeInvariantIndexStrings.Format(index)}";

        public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired) => unmodifiedType;

        public string GetPinnedType(string elementType) => elementType;

        public string GetPointerType(string elementType) => $"{elementType}*";

        public string GetPrimitiveType(PrimitiveTypeCode typeCode)
        {
            return typeCode switch
            {
                PrimitiveTypeCode.Boolean => "System.Boolean",
                PrimitiveTypeCode.Byte => "System.Byte",
                PrimitiveTypeCode.Char => "System.Char",
                PrimitiveTypeCode.Double => "System.Double",
                PrimitiveTypeCode.Int16 => "System.Int16",
                PrimitiveTypeCode.Int32 => "System.Int32",
                PrimitiveTypeCode.Int64 => "System.Int64",
                PrimitiveTypeCode.IntPtr => "System.IntPtr",
                PrimitiveTypeCode.Object => "System.Object",
                PrimitiveTypeCode.SByte => "System.SByte",
                PrimitiveTypeCode.Single => "System.Single",
                PrimitiveTypeCode.String => "System.String",
                PrimitiveTypeCode.TypedReference => "System.TypedReference",
                PrimitiveTypeCode.UInt16 => "System.UInt16",
                PrimitiveTypeCode.UInt32 => "System.UInt32",
                PrimitiveTypeCode.UInt64 => "System.UInt64",
                PrimitiveTypeCode.UIntPtr => "System.UIntPtr",
                PrimitiveTypeCode.Void => "System.Void",
                _ => "System.Object",
            };
        }

        public string GetSZArrayType(string elementType) => $"{elementType}[]";

        public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => GetTypeIdentity(reader, reader.GetTypeDefinition(handle));

        public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) => GetFullTypeReferenceName(reader, handle);

        public string GetTypeFromSpecification(MetadataReader reader, object? genericContext, TypeSpecificationHandle handle, byte rawTypeKind) =>
            reader.GetTypeSpecification(handle).DecodeSignature(this, genericContext);

        internal static string FormatTypeList(ImmutableArray<string> types)
        {
            if (types.Length == 0)
                return string.Empty;

            var parts = new string[types.Length];
            for (var i = 0; i < parts.Length; i++)
                parts[i] = types[i];

            return JoinCommaSeparated(parts);
        }

        internal static string GetFullTypeReferenceName(MetadataReader reader, TypeReferenceHandle handle)
        {
            var typeRef = reader.GetTypeReference(handle);
            var name = reader.GetString(typeRef.Name);
            var scope = typeRef.ResolutionScope;
            if (scope.Kind == HandleKind.TypeReference)
                return $"{GetFullTypeReferenceName(reader, (TypeReferenceHandle)scope)}.{name}";

            if (scope.Kind == HandleKind.TypeDefinition)
            {
                var enclosingHandle = (TypeDefinitionHandle)scope;
                return $"{GetTypeIdentity(reader, reader.GetTypeDefinition(enclosingHandle))}.{name}";
            }

            var ns = reader.GetString(typeRef.Namespace);
            return string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";
        }

        internal static string GetTypeIdentity(MetadataReader reader, TypeDefinition typeDef)
        {
            var name = reader.GetString(typeDef.Name);
            var declaring = typeDef.GetDeclaringType();
            if (declaring.IsNil)
            {
                var ns = reader.GetString(typeDef.Namespace);
                return string.IsNullOrEmpty(ns) ? name : $"{ns}.{name}";
            }

            return $"{GetTypeIdentity(reader, reader.GetTypeDefinition(declaring))}.{name}";
        }

        private static string JoinCommaSeparated(string[] parts) => string.Join(',', parts);
    }
}
