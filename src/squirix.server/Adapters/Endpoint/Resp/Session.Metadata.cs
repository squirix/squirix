using System;
using System.Globalization;
using System.Text;

namespace Squirix.Server.Adapters.Endpoint.Resp;

/// <content>Holds the RESP connection metadata.</content>
internal sealed partial class Session
{
    private sealed class Metadata
    {
        private const string ServerVersion = BuildMetadata.InformationalVersion;

        private static readonly byte[] ServerInfo = [.. "# Server\r\nredis_mode:standalone\r\n"u8];
        private static readonly byte[] ReplicationInfo = [.. "# Replication\r\nrole:master\r\nconnected_slaves:0\r\n"u8];
        private static readonly byte[] ClusterInfo = [.. "# Cluster\r\ncluster_enabled:0\r\n"u8];
        private static readonly byte[] AllInfo = [.. ServerInfo, .. "\r\n"u8, .. ReplicationInfo, .. "\r\n"u8, .. ClusterInfo];

        private readonly long _id;

        internal Metadata(long id)
        {
            _id = id;
        }

        internal ReadOnlyMemory<byte> Name { private get; set; }

        internal static Response Info(RequestReader request) => request.Count switch
        {
            1 => Response.Bulk(AllInfo),
            2 when request.IsCommand(1, "server"u8) => Response.Bulk(ServerInfo),
            2 when request.IsCommand(1, "replication"u8) => Response.Bulk(ReplicationInfo),
            2 when request.IsCommand(1, "cluster"u8) => Response.Bulk(ClusterInfo),
            2 => Response.Bulk(ReadOnlyMemory<byte>.Empty),
            _ => Response.Arity,
        };

        internal static bool IsValidName(ReadOnlySpan<byte> name) =>
            name.Length <= 256 && (name.IsEmpty || !name.ContainsAnyExceptInRange("!"u8[0], "~"u8[0]));

        internal Response Client(RequestReader request) => request.Count switch
        {
            2 when request.IsCommand(1, "ID"u8) => Response.Integer(_id),
            2 when request.IsCommand(1, "GETNAME"u8) => Name.IsEmpty ? default : Response.Bulk(Name),
            3 when request.IsCommand(1, "SETNAME"u8) => SetName(request[2]),
            4 when request.IsCommand(1, "SETINFO"u8) => SetInfo(request),
            _ => Response.Unknown,
        };

        internal Response Hello(bool resp3)
        {
            var header = resp3 ? "%7\r\n" : "*14\r\n";
            var version = resp3 ? 3 : 2;
            var metadata = string.Create(
                CultureInfo.InvariantCulture,
                $"{header}$6\r\nserver\r\n$7\r\nsquirix\r\n$7\r\nversion\r\n${ServerVersion.Length}\r\n{ServerVersion}\r\n$5\r\nproto\r\n:{version}\r\n$2\r\nid\r\n:{_id}\r\n$4\r\nmode\r\n$10\r\nstandalone\r\n$4\r\nrole\r\n$6\r\nmaster\r\n$7\r\nmodules\r\n*0\r\n");
            return Response.Encoded(Encoding.ASCII.GetBytes(metadata));
        }

        private static Response SetInfo(RequestReader request)
        {
            var validAttribute = request.IsCommand(2, "LIB-NAME"u8) || request.IsCommand(2, "LIB-VER"u8);
            return validAttribute && IsValidName(request[3].Span) ? Response.Ok : Response.Syntax;
        }

        private Response SetName(ReadOnlyMemory<byte> name)
        {
            if (!IsValidName(name.Span))
                return Response.Syntax;
            Name = name.ToArray();
            return Response.Ok;
        }
    }
}
