using System.Threading.Tasks;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

internal interface IBinaryAuthorizationHook
{
    ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request);
}
