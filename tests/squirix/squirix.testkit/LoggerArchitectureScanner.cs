using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.IO;
using System.Reflection;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;

namespace Squirix.TestKit;

/// <summary>
/// Metadata scan of an assembly for the explicit-logger rules: loggers reach their user only through required constructor or method
/// arguments, never through static state, optional or nullable parameters and members, or settable properties.
/// </summary>
/// <remarks>
/// The assembly is read with System.Reflection.Metadata and logger types are matched by full name, so the scan needs no reference to the
/// logging abstractions: ILogger, ILogger of T, ILoggerFactory, ILoggerProvider and every type declared in the scanned assembly that implements
/// one of them. All types are scanned, nested and non-public ones included, declared members only.
/// Compiler-generated types and members (closures, state machines, backing fields) are skipped: their nullability is not annotated and they only
/// mirror members that are scanned in their declared form. Source-generated logging methods are exempt from the static-class parameter rule
/// because they are pure and take the logger as their first argument. Arrays and by-reference forms of a logger type count as logger types.
/// </remarks>
public static class LoggerArchitectureScanner
{
    private const string LoggerInterfaceName = "Microsoft.Extensions.Logging.ILogger";
    private const string GenericLoggerInterfaceName = "Microsoft.Extensions.Logging.ILogger`1";
    private const string LoggerFactoryInterfaceName = "Microsoft.Extensions.Logging.ILoggerFactory";
    private const string LoggerProviderInterfaceName = "Microsoft.Extensions.Logging.ILoggerProvider";
    private const string LoggerMessageAttributeName = "Microsoft.Extensions.Logging.LoggerMessageAttribute";
    private const string CompilerGeneratedAttributeName = "System.Runtime.CompilerServices.CompilerGeneratedAttribute";
    private const string NullableAttributeName = "System.Runtime.CompilerServices.NullableAttribute";
    private const string NullableContextAttributeName = "System.Runtime.CompilerServices.NullableContextAttribute";
    private const byte NullableFlag = 2;

    /// <summary>Finds every static member that holds or returns a logger type.</summary>
    /// <param name="assemblyPath">Absolute path to the assembly under test.</param>
    /// <returns>One description per violation.</returns>
    public static IReadOnlyList<string> FindStaticLoggerMembers(string assemblyPath)
    {
        using var model = AssemblyModel.Load(assemblyPath);
        var violations = new List<string>();
        foreach (var typeHandle in model.Reader.TypeDefinitions)
        {
            if (model.IsCompilerGeneratedType(typeHandle))
                continue;

            var type = model.Reader.GetTypeDefinition(typeHandle);
            var typeName = model.NameOf(typeHandle);
            foreach (var fieldHandle in type.GetFields())
                FindStaticField(model, fieldHandle, typeName, violations);

            foreach (var propertyHandle in type.GetProperties())
                FindStaticProperty(model, propertyHandle, typeName, violations);

            foreach (var methodHandle in type.GetMethods())
                FindStaticMethod(model, methodHandle, typeName, violations);
        }

        return violations;
    }

    /// <summary>Finds every static class method that takes an ILogger and is not a source-generated logging method.</summary>
    /// <param name="assemblyPath">Absolute path to the assembly under test.</param>
    /// <returns>One description per violation.</returns>
    public static IReadOnlyList<string> FindStaticClassLoggerParameters(string assemblyPath)
    {
        using var model = AssemblyModel.Load(assemblyPath);
        var violations = new List<string>();
        foreach (var typeHandle in model.Reader.TypeDefinitions)
        {
            if (!IsStaticClass(model.Reader.GetTypeDefinition(typeHandle)) || model.IsCompilerGeneratedType(typeHandle))
                continue;

            var typeName = model.NameOf(typeHandle);
            foreach (var methodHandle in model.Reader.GetTypeDefinition(typeHandle).GetMethods())
                FindLoggerParameters(model, methodHandle, typeName, violations);
        }

        return violations;
    }

    /// <summary>Finds optional or nullable logger parameters, nullable logger fields and properties, and logger properties with a setter or init accessor.</summary>
    /// <param name="assemblyPath">Absolute path to the assembly under test.</param>
    /// <returns>One description per violation.</returns>
    public static IReadOnlyList<string> FindOptionalLoggers(string assemblyPath)
    {
        using var model = AssemblyModel.Load(assemblyPath);
        var violations = new List<string>();
        foreach (var typeHandle in model.Reader.TypeDefinitions)
        {
            if (model.IsCompilerGeneratedType(typeHandle))
                continue;

            var type = model.Reader.GetTypeDefinition(typeHandle);
            var typeName = model.NameOf(typeHandle);
            foreach (var methodHandle in type.GetMethods())
                FindOptionalParameters(model, typeHandle, methodHandle, typeName, violations);

            foreach (var fieldHandle in type.GetFields())
                FindNullableField(model, typeHandle, fieldHandle, typeName, violations);

            foreach (var propertyHandle in type.GetProperties())
                FindNullableOrSettableProperty(model, typeHandle, propertyHandle, typeName, violations);
        }

        return violations;
    }

    private static bool IsStaticClass(TypeDefinition type)
    {
        const TypeAttributes staticClass = TypeAttributes.Abstract | TypeAttributes.Sealed;
        return (type.Attributes & staticClass) == staticClass && !type.Attributes.HasFlag(TypeAttributes.Interface);
    }

    private static void FindStaticField(AssemblyModel model, FieldDefinitionHandle fieldHandle, string typeName, List<string> violations)
    {
        var field = model.Reader.GetFieldDefinition(fieldHandle);
        var name = model.Reader.GetString(field.Name);
        if (!field.Attributes.HasFlag(FieldAttributes.Static) || IsGeneratedName(name) || model.HasAttribute(field.GetCustomAttributes(), CompilerGeneratedAttributeName))
            return;

        if (model.IsLogger(field.DecodeSignature(model.Types, null), false))
            violations.Add($"{typeName}.{name}: a static field must not hold a logger.");
    }

    private static void FindStaticProperty(AssemblyModel model, PropertyDefinitionHandle propertyHandle, string typeName, List<string> violations)
    {
        var property = model.Reader.GetPropertyDefinition(propertyHandle);
        var accessors = property.GetAccessors();
        var accessor = accessors.Getter.IsNil ? accessors.Setter : accessors.Getter;
        if (accessor.IsNil || !model.Reader.GetMethodDefinition(accessor).Attributes.HasFlag(MethodAttributes.Static))
            return;

        if (model.IsLogger(property.DecodeSignature(model.Types, null).ReturnType, false))
            violations.Add($"{typeName}.{model.Reader.GetString(property.Name)}: a static property must not hold a logger.");
    }

    private static void FindStaticMethod(AssemblyModel model, MethodDefinitionHandle methodHandle, string typeName, List<string> violations)
    {
        var method = model.Reader.GetMethodDefinition(methodHandle);
        var name = model.Reader.GetString(method.Name);
        if (!method.Attributes.HasFlag(MethodAttributes.Static) || IsGeneratedName(name) || model.HasAttribute(method.GetCustomAttributes(), CompilerGeneratedAttributeName))
            return;

        if (model.IsLogger(method.DecodeSignature(model.Types, null).ReturnType, false))
            violations.Add($"{typeName}.{name}: a static method must not return a logger.");
    }

    private static void FindLoggerParameters(AssemblyModel model, MethodDefinitionHandle methodHandle, string typeName, List<string> violations)
    {
        var method = model.Reader.GetMethodDefinition(methodHandle);
        var name = model.Reader.GetString(method.Name);
        if (IsGeneratedName(name) || model.HasAttribute(method.GetCustomAttributes(), CompilerGeneratedAttributeName)
            || model.HasAttribute(method.GetCustomAttributes(), LoggerMessageAttributeName))
            return;

        var parameterTypes = method.DecodeSignature(model.Types, null).ParameterTypes;
        for (var index = 0; index < parameterTypes.Length; index++)
        {
            if (model.IsLogger(parameterTypes[index], true))
                violations.Add($"{typeName}.{name}: a static class must not take a parameter of type {parameterTypes[index]}.");
        }
    }

    private static void FindNullableField(AssemblyModel model, TypeDefinitionHandle typeHandle, FieldDefinitionHandle fieldHandle, string typeName, List<string> violations)
    {
        var field = model.Reader.GetFieldDefinition(fieldHandle);
        var name = model.Reader.GetString(field.Name);
        if (IsGeneratedName(name) || model.HasAttribute(field.GetCustomAttributes(), CompilerGeneratedAttributeName))
            return;

        if (model.IsLogger(field.DecodeSignature(model.Types, null), false) && model.IsNullable(field.GetCustomAttributes(), typeHandle, default))
            violations.Add($"{typeName}.{name}: a logger field must not be nullable.");
    }

    private static void FindNullableOrSettableProperty(
        AssemblyModel model,
        TypeDefinitionHandle typeHandle,
        PropertyDefinitionHandle propertyHandle,
        string typeName,
        List<string> violations)
    {
        var property = model.Reader.GetPropertyDefinition(propertyHandle);
        if (!model.IsLogger(property.DecodeSignature(model.Types, null).ReturnType, false))
            return;

        var name = model.Reader.GetString(property.Name);
        if (model.IsNullable(property.GetCustomAttributes(), typeHandle, default))
            violations.Add($"{typeName}.{name}: a logger property must not be nullable.");

        if (!property.GetAccessors().Setter.IsNil)
            violations.Add($"{typeName}.{name}: a logger property must not have a setter or init accessor.");
    }

    private static void FindOptionalParameters(AssemblyModel model, TypeDefinitionHandle typeHandle, MethodDefinitionHandle methodHandle, string typeName, List<string> violations)
    {
        var method = model.Reader.GetMethodDefinition(methodHandle);
        var methodName = model.Reader.GetString(method.Name);
        if (IsGeneratedName(methodName) || model.HasAttribute(method.GetCustomAttributes(), CompilerGeneratedAttributeName))
            return;

        var parameterTypes = method.DecodeSignature(model.Types, null).ParameterTypes;
        foreach (var parameterHandle in method.GetParameters())
        {
            var parameter = model.Reader.GetParameter(parameterHandle);
            var position = parameter.SequenceNumber - 1;
            if (position < 0 || position >= parameterTypes.Length || !model.IsLogger(parameterTypes[position], false))
                continue;

            var parameterName = model.Reader.GetString(parameter.Name);
            if ((parameter.Attributes & (ParameterAttributes.Optional | ParameterAttributes.HasDefault)) != ParameterAttributes.None)
                violations.Add($"{typeName}.{methodName}: logger parameter '{parameterName}' must not be optional.");

            if (model.IsNullable(parameter.GetCustomAttributes(), typeHandle, methodHandle))
                violations.Add($"{typeName}.{methodName}: logger parameter '{parameterName}' must not be nullable.");
        }
    }

    private static bool IsGeneratedName(string name) => name.Contains('<', StringComparison.Ordinal);

    private sealed class AssemblyModel : IDisposable
    {
        private readonly PEReader _peReader;
        private readonly Dictionary<string, bool> _implementers;

        private AssemblyModel(PEReader peReader)
        {
            _peReader = peReader;
            _implementers = [with(StringComparer.Ordinal)];
            Reader = peReader.GetMetadataReader();
            Types = new LoggerTypeProvider(this);
            CollectImplementers();
        }

        internal MetadataReader Reader { get; }

        internal LoggerTypeProvider Types { get; }

        public void Dispose() => _peReader.Dispose();

        internal static AssemblyModel Load(string assemblyPath)
        {
            ArgumentException.ThrowIfNullOrEmpty(assemblyPath);

            var peReader = new PEReader(File.OpenRead(assemblyPath));
            if (peReader.HasMetadata)
                return new AssemblyModel(peReader);

            peReader.Dispose();
            throw new InvalidOperationException($"Could not load metadata from '{assemblyPath}'.");
        }

        internal string NameOf(TypeDefinitionHandle handle)
        {
            var type = Reader.GetTypeDefinition(handle);
            var name = Reader.GetString(type.Name);
            var declaring = type.GetDeclaringType();
            if (!declaring.IsNil)
                return $"{NameOf(declaring)}/{name}";

            var typeNamespace = Reader.GetString(type.Namespace);
            return typeNamespace.Length == 0 ? name : $"{typeNamespace}.{name}";
        }

        internal string NameOf(TypeReferenceHandle handle)
        {
            var type = Reader.GetTypeReference(handle);
            var name = Reader.GetString(type.Name);
            var typeNamespace = Reader.GetString(type.Namespace);
            return typeNamespace.Length == 0 ? name : $"{typeNamespace}.{name}";
        }

        internal bool IsCompilerGeneratedType(TypeDefinitionHandle handle)
        {
            for (var current = handle; !current.IsNil; current = Reader.GetTypeDefinition(current).GetDeclaringType())
            {
                var type = Reader.GetTypeDefinition(current);
                if (IsGeneratedName(Reader.GetString(type.Name)) || HasAttribute(type.GetCustomAttributes(), CompilerGeneratedAttributeName))
                    return true;
            }

            return false;
        }

        internal bool HasAttribute(CustomAttributeHandleCollection attributes, string attributeFullName)
        {
            foreach (var handle in attributes)
            {
                if (string.Equals(AttributeName(Reader.GetCustomAttribute(handle)), attributeFullName, StringComparison.Ordinal))
                    return true;
            }

            return false;
        }

        internal bool IsLogger(string typeName, bool instanceOnly)
        {
            var isInstanceLogger = TryGetInterfaceKind(typeName, out var isInterfaceInstanceLogger)
                ? isInterfaceInstanceLogger
                : _implementers.TryGetValue(typeName, out var implementsInstanceLogger) && implementsInstanceLogger;
            return isInstanceLogger || (!instanceOnly && (TryGetInterfaceKind(typeName, out _) || _implementers.ContainsKey(typeName)));
        }

        internal bool IsNullable(CustomAttributeHandleCollection ownAttributes, TypeDefinitionHandle typeHandle, MethodDefinitionHandle methodHandle)
        {
            if (TryReadNullableFlag(ownAttributes, NullableAttributeName, out var flag))
                return flag == NullableFlag;

            if (!methodHandle.IsNil && TryReadNullableFlag(Reader.GetMethodDefinition(methodHandle).GetCustomAttributes(), NullableContextAttributeName, out flag))
                return flag == NullableFlag;

            for (var current = typeHandle; !current.IsNil; current = Reader.GetTypeDefinition(current).GetDeclaringType())
            {
                if (TryReadNullableFlag(Reader.GetTypeDefinition(current).GetCustomAttributes(), NullableContextAttributeName, out flag))
                    return flag == NullableFlag;
            }

            return false;
        }

        private static bool TryGetInterfaceKind(string typeName, out bool isInstanceLogger)
        {
            isInstanceLogger = string.Equals(typeName, LoggerInterfaceName, StringComparison.Ordinal) || string.Equals(typeName, GenericLoggerInterfaceName, StringComparison.Ordinal);
            return isInstanceLogger || string.Equals(typeName, LoggerFactoryInterfaceName, StringComparison.Ordinal) || string.Equals(typeName, LoggerProviderInterfaceName, StringComparison.Ordinal);
        }

        private string NameOfEntity(EntityHandle handle) =>
            handle.Kind == HandleKind.TypeDefinition ? NameOf((TypeDefinitionHandle)handle) : NameOfSpecificationOrReference(handle);

        private string NameOfSpecificationOrReference(EntityHandle handle) =>
            handle.Kind == HandleKind.TypeSpecification
                ? Reader.GetTypeSpecification((TypeSpecificationHandle)handle).DecodeSignature(Types, null)
                : NameOfReference(handle);

        private string NameOfReference(EntityHandle handle) => handle.Kind == HandleKind.TypeReference ? NameOf((TypeReferenceHandle)handle) : string.Empty;

        private string AttributeName(CustomAttribute attribute)
        {
            if (attribute.Constructor.Kind == HandleKind.MethodDefinition)
                return NameOf(Reader.GetMethodDefinition((MethodDefinitionHandle)attribute.Constructor).GetDeclaringType());

            if (attribute.Constructor.Kind != HandleKind.MemberReference)
                return string.Empty;

            var parent = Reader.GetMemberReference((MemberReferenceHandle)attribute.Constructor).Parent;
            return parent.Kind == HandleKind.TypeReference ? NameOf((TypeReferenceHandle)parent) : string.Empty;
        }

        private bool TryReadNullableFlag(CustomAttributeHandleCollection attributes, string attributeFullName, out byte flag)
        {
            foreach (var handle in attributes)
            {
                var attribute = Reader.GetCustomAttribute(handle);
                if (!string.Equals(AttributeName(attribute), attributeFullName, StringComparison.Ordinal))
                    continue;

                var blob = Reader.GetBlobReader(attribute.Value);
                _ = blob.ReadUInt16();

                // A single-byte argument is followed only by the two-byte named-argument count; otherwise the first byte of the array is the top-level flag.
                if (blob.RemainingBytes != sizeof(byte) + sizeof(ushort) && blob.ReadInt32() == 0)
                    continue;

                flag = blob.ReadByte();
                return true;
            }

            flag = 0;
            return false;
        }

        private void CollectImplementers()
        {
            var changed = true;
            while (changed)
            {
                changed = false;
                foreach (var typeHandle in Reader.TypeDefinitions)
                    changed |= RecordImplementer(typeHandle);
            }
        }

        private bool RecordImplementer(TypeDefinitionHandle typeHandle)
        {
            var type = Reader.GetTypeDefinition(typeHandle);
            var name = NameOf(typeHandle);
            var isLogger = _implementers.TryGetValue(name, out var isInstanceLogger);
            foreach (var interfaceHandle in type.GetInterfaceImplementations())
                Consider(Reader.GetInterfaceImplementation(interfaceHandle).Interface, ref isLogger, ref isInstanceLogger);

            if (!type.BaseType.IsNil)
                Consider(type.BaseType, ref isLogger, ref isInstanceLogger);

            if (!isLogger || (_implementers.TryGetValue(name, out var existing) && existing == isInstanceLogger))
                return false;

            _implementers[name] = isInstanceLogger;
            return true;
        }

        private void Consider(EntityHandle handle, ref bool isLogger, ref bool isInstanceLogger)
        {
            var name = NameOfEntity(handle);
            if (!IsLogger(name, false))
                return;

            isLogger = true;
            isInstanceLogger |= IsLogger(name, true);
        }
    }

    private sealed class LoggerTypeProvider : ISignatureTypeProvider<string, object?>
    {
        private readonly AssemblyModel _model;

        internal LoggerTypeProvider(AssemblyModel model)
        {
            _model = model;
        }

        public string GetArrayType(string elementType, ArrayShape shape) => elementType;

        public string GetByReferenceType(string elementType) => elementType;

        public string GetFunctionPointerType(MethodSignature<string> signature) => "fnptr";

        public string GetGenericInstantiation(string genericType, ImmutableArray<string> typeArguments) => genericType;

        public string GetGenericMethodParameter(object? genericContext, int index) => "!!";

        public string GetGenericTypeParameter(object? genericContext, int index) => "!";

        public string GetModifiedType(string modifier, string unmodifiedType, bool isRequired) => unmodifiedType;

        public string GetPinnedType(string elementType) => elementType;

        public string GetPointerType(string elementType) => elementType;

        public string GetPrimitiveType(PrimitiveTypeCode typeCode) => "primitive";

        public string GetSZArrayType(string elementType) => elementType;

        public string GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => _model.NameOf(handle);

        public string GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) => _model.NameOf(handle);

        public string GetTypeFromSpecification(MetadataReader reader, object? genericContext, TypeSpecificationHandle handle, byte rawTypeKind) =>
            reader.GetTypeSpecification(handle).DecodeSignature(this, genericContext);
    }
}
