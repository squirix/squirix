namespace Squirix.Server.Threading;

/// <summary>Unit of synchronous work executed on a pool thread by <see cref="WorkPool"/>.</summary>
/// <remarks>
/// Implementations must always complete, so any owned buffers or handles are released, even when the scheduling
/// token is <c language="csharp">CancellationToken.None</c>. The work is passed as a reference state object, so implementers
/// must be reference types.
/// </remarks>
internal interface IWorkPoolItem
{
    void Execute();
}
