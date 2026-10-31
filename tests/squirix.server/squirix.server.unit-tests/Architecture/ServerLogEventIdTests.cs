using System;
using System.Collections.Generic;
using System.IO;
using System.Reflection.Metadata;
using System.Reflection.PortableExecutable;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Architecture;

/// <summary>Every server logging method must declare a unique event id.</summary>
[Immutable]
public sealed class ServerLogEventIdTests
{
    /// <summary>Checks the actual logging attributes, independently of source formatting.</summary>
    [Test]
    public async Task EventIdsShouldBeUnique()
    {
        await using var stream = File.OpenRead(Path.Join(AppContext.BaseDirectory, "Squirix.Server.dll"));
        using var pe = new PEReader(stream);
        var reader = pe.GetMetadataReader();
        var owners = new Dictionary<int, string>();
        var violations = new List<string>();
        var count = 0;
        foreach (var handle in reader.MethodDefinitions)
        {
            var method = reader.GetMethodDefinition(handle);
            foreach (var attributeHandle in method.GetCustomAttributes())
            {
                var attribute = reader.GetCustomAttribute(attributeHandle);
                if (!IsLoggerAttribute(reader, attribute))
                    continue;

                count++;
                var id = ReadEventId(attribute.DecodeValue(new AttributeTypes()));
                var owner = reader.GetTypeDefinition(method.GetDeclaringType());
                var name = $"{reader.GetString(owner.Namespace)}.{reader.GetString(owner.Name)}.{reader.GetString(method.Name)}";
                if (id == null)
                    violations.Add($"{name} has no explicit EventId.");
                else if (!owners.TryAdd(id.Value, name))
                    violations.Add($"{name} reuses event id {id} of {owners[id.Value]}.");
            }
        }

        _ = await Assert.That(count).IsGreaterThan(0);
        _ = await Assert.That(violations).IsEmpty().Because(string.Join(Environment.NewLine, violations));
    }

    private static bool IsLoggerAttribute(MetadataReader reader, CustomAttribute attribute)
    {
        if (attribute.Constructor.Kind != HandleKind.MemberReference)
            return false;
        var parent = reader.GetMemberReference((MemberReferenceHandle)attribute.Constructor).Parent;
        if (parent.Kind != HandleKind.TypeReference)
            return false;
        var type = reader.GetTypeReference((TypeReferenceHandle)parent);
        return reader.StringComparer.Equals(type.Namespace, "Microsoft.Extensions.Logging") &&
               reader.StringComparer.Equals(type.Name, "LoggerMessageAttribute");
    }

    private static int? ReadEventId(CustomAttributeValue<PrimitiveTypeCode> value)
    {
        foreach (var argument in value.NamedArguments)
        {
            if (string.Equals(argument.Name, "EventId", StringComparison.Ordinal) && argument.Value is int named)
                return named;
        }

        return value.FixedArguments.Length > 0 && value.FixedArguments[0].Type == PrimitiveTypeCode.Int32 && value.FixedArguments[0].Value is int positional
            ? positional
            : null;
    }

    private sealed class AttributeTypes : ICustomAttributeTypeProvider<PrimitiveTypeCode>
    {
        public PrimitiveTypeCode GetPrimitiveType(PrimitiveTypeCode typeCode) => typeCode;

        public PrimitiveTypeCode GetSystemType() => PrimitiveTypeCode.Object;

        public bool IsSystemType(PrimitiveTypeCode type) => type == PrimitiveTypeCode.Object;

        public PrimitiveTypeCode GetSZArrayType(PrimitiveTypeCode elementType) => elementType;

        public PrimitiveTypeCode GetTypeFromDefinition(MetadataReader reader, TypeDefinitionHandle handle, byte rawTypeKind) => PrimitiveTypeCode.Object;

        public PrimitiveTypeCode GetTypeFromReference(MetadataReader reader, TypeReferenceHandle handle, byte rawTypeKind) => PrimitiveTypeCode.Object;

        public PrimitiveTypeCode GetTypeFromSerializedName(string name) => PrimitiveTypeCode.Int32;

        public PrimitiveTypeCode GetUnderlyingEnumType(PrimitiveTypeCode type) => PrimitiveTypeCode.Int32;
    }
}
