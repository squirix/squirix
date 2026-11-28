using System.Threading.Tasks;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Authorizes a request on its connection's receive path.</summary>
/// <remarks>Return promptly. Implementations that wait for I/O must return an incomplete task so the reader can process subsequent frames.</remarks>
internal interface IBinaryAuthorizationHook
{
    ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request);
}
