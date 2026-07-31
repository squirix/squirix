namespace Squirix.ProtocolModel;

internal static class VoteMask
{
    internal static int CountGranted(int voteMask) => int.PopCount(voteMask);

    internal static int Remap(int voteMask, int[] map)
    {
        var remapped = 0;
        for (var oldId = 0; oldId < map.Length; oldId++)
        {
            if ((voteMask & (1 << oldId)) is 0)
                continue;

            remapped |= 1 << map[oldId];
        }

        return remapped;
    }
}
