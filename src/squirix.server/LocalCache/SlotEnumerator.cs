namespace Squirix.Server.LocalCache;

/// <summary>Visits occupied slots in page order under the cache lock, including while buckets are migrating.</summary>
/// <typeparam name="T">The stored value type.</typeparam>
internal sealed class SlotEnumerator<T>
{
    private readonly SlotPage<T>?[] _pages;
    private readonly int _pageCount;
    private SlotPage<T>? _page;
    private int _pageIndex = -1;
    private int _offset;

    internal SlotEnumerator(SlotPage<T>?[] pages, int pageCount)
    {
        _pages = pages;
        _pageCount = pageCount;
    }

    /// <summary>Gets the occupied slot numbered page index × page capacity + offset + 1.</summary>
    internal CacheSlot<T> Current { get; private set; }

    internal bool MoveNext()
    {
        while (true)
        {
            while (_page != null && _offset < _page.Length)
            {
                var offset = _offset++;
                var identity = _page.ReadIdentity(offset);
                if (identity <= 0)
                    continue;
                Current = new CacheSlot<T>(_page, (_pageIndex * SlotPageBase.Capacity) + offset + 1, identity);
                return true;
            }

            if (++_pageIndex >= _pageCount)
                return false;

            _page = _pages[_pageIndex];
            _offset = 0;
        }
    }
}
