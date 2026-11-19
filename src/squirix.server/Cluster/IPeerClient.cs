using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Squirix.Server.Cluster;

/// <summary>Sends one protobuf operation to a configured peer.</summary>
internal interface IPeerClient
{
    Task<T> SendAsync<T>(string path, IMessage request, MessageParser<T> parser, CancellationToken cancellationToken)
        where T : IMessage<T>;
}
