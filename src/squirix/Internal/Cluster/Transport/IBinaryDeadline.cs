namespace Squirix.Internal.Cluster.Transport;

internal interface IBinaryDeadline
{
    long Expires { get; }
    bool IsLive { get; }

    void Expire(long observed);
}
