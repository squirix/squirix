using Squirix.Transport.Binary;

namespace Squirix.Server.Errors;

internal static class SquirixExceptionExtensions
{
    extension(SquirixException exception)
    {
        internal ServerCallException ToServerCallException() => new(SquirixErrorMapper.ToFrameStatus(exception.Code), exception.Detail ?? exception.Error);
    }
}
