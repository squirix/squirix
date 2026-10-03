namespace Squirix.Server.Runtime;

/// <summary>Observer of a scope that is told when one of its mutation frames is stamped with the write-ahead operation id.</summary>
internal interface IRpcMutationStampListener
{
    /// <summary>Called, under the journal mutation gate, after a stamped mutation frame was enqueued.</summary>
    void OnMutationStamped();
}
