namespace EventIngestion.Core.Storage;

using EventIngestion.Core.Models;

// Ring buffer that stores sensor readings in chronological order for one device.
// Automatically reuses memory slots as old readings expire past 5 minutes.
public sealed class DeviceCircularBuffer
{
    private const int DefaultInitialCapacity = 64;

    private ReadingEntry[] _entries;
    private int _head;
    private int _count;

    private double _sum;
    private double _cachedMin = double.MaxValue;
    private double _cachedMax = double.MinValue;
    private bool _minMaxDirty = false;

    // Creates the buffer with an initial capacity (minimum 8 slots).
    public DeviceCircularBuffer(int initialCapacity = DefaultInitialCapacity)
    {
        int capacity = Math.Max(initialCapacity, 8);
        _entries = new ReadingEntry[capacity];
        _head = 0;
        _count = 0;
        _sum = 0;
    }

    // Number of active readings currently within the 5-minute window.
    public int Count
    {
        get
        {
            return _count;
        }
    }

    // Adds a new reading into the buffer. Drops it if it's already older than 5 minutes.
    // Handles in-order data in O(1) time, and sorts out-of-order data if needed.
    public bool TryInsert(ReadingEntry reading, long cutoffUtcMs)
    {
        if (reading.TimestampUtcMs < cutoffUtcMs)
        {
            return false;
        }

        PruneOlderThan(cutoffUtcMs);

        if (_count == 0)
        {
            _entries[0] = reading;
            _head = 0;
            _count = 1;
            _sum = reading.Value;
            _cachedMin = reading.Value;
            _cachedMax = reading.Value;
            _minMaxDirty = false;
            return true;
        }

        if (_count == _entries.Length)
        {
            GrowBuffer();
        }

        int tailPhysical = (_head + _count - 1) % _entries.Length;
        ReadingEntry newest = _entries[tailPhysical];

        if (reading.TimestampUtcMs >= newest.TimestampUtcMs)
        {
            int insertIndex = (_head + _count) % _entries.Length;
            _entries[insertIndex] = reading;
            _count++;
            _sum += reading.Value;

            if (!_minMaxDirty)
            {
                if (reading.Value < _cachedMin)
                {
                    _cachedMin = reading.Value;
                }
                if (reading.Value > _cachedMax)
                {
                    _cachedMax = reading.Value;
                }
            }
            return true;
        }

        InsertOutOfOrder(reading);
        return true;
    }

    // Places a late reading into its correct chronological position by shifting newer items right.
    private void InsertOutOfOrder(ReadingEntry reading)
    {
        int insertLogical = _count;
        for (int i = _count - 1; i >= 0; i--)
        {
            int physIdx = (_head + i) % _entries.Length;
            if (_entries[physIdx].TimestampUtcMs <= reading.TimestampUtcMs)
            {
                insertLogical = i + 1;
                break;
            }
            if (i == 0)
            {
                insertLogical = 0;
            }
        }

        for (int i = _count - 1; i >= insertLogical; i--)
        {
            int srcPhys = (_head + i) % _entries.Length;
            int dstPhys = (_head + i + 1) % _entries.Length;
            _entries[dstPhys] = _entries[srcPhys];
        }

        int targetPhys = (_head + insertLogical) % _entries.Length;
        _entries[targetPhys] = reading;
        _count++;
        _sum += reading.Value;

        if (!_minMaxDirty)
        {
            if (reading.Value < _cachedMin)
            {
                _cachedMin = reading.Value;
            }
            if (reading.Value > _cachedMax)
            {
                _cachedMax = reading.Value;
            }
        }
    }

    // Removes readings older than the cutoff timestamp by advancing the head pointer.
    public int PruneOlderThan(long cutoffUtcMs)
    {
        int pruned = 0;
        while (_count > 0)
        {
            ref readonly ReadingEntry oldest = ref _entries[_head];
            if (oldest.TimestampUtcMs >= cutoffUtcMs)
            {
                break;
            }

            double val = oldest.Value;
            _sum -= val;

            if (!_minMaxDirty && (val == _cachedMin || val == _cachedMax))
            {
                _minMaxDirty = true;
            }

            _head = (_head + 1) % _entries.Length;
            _count--;
            pruned++;
        }

        if (_count == 0)
        {
            _head = 0;
            _sum = 0;
            _cachedMin = double.MaxValue;
            _cachedMax = double.MinValue;
            _minMaxDirty = false;
        }

        return pruned;
    }

    // Returns count, min, max, and average for this device. Recalculates min/max if marked dirty.
    public bool TryGetAggregates(out DeviceAggregates aggregates)
    {
        if (_count == 0)
        {
            aggregates = default;
            return false;
        }

        if (_minMaxDirty)
        {
            RecalculateMinMax();
        }

        double average = _count > 0 ? _sum / _count : 0.0;
        aggregates = new DeviceAggregates(
            Count: _count,
            Min: _cachedMin,
            Max: _cachedMax,
            Average: average
        );
        return true;
    }

    // Scans the active readings to find new min and max values only when needed.
    private void RecalculateMinMax()
    {
        double min = double.MaxValue;
        double max = double.MinValue;

        for (int i = 0; i < _count; i++)
        {
            int physIdx = (_head + i) % _entries.Length;
            double v = _entries[physIdx].Value;
            if (v < min)
            {
                min = v;
            }
            if (v > max)
            {
                max = v;
            }
        }

        _cachedMin = min;
        _cachedMax = max;
        _minMaxDirty = false;
    }

    // Doubles the array size when full, keeping the existing readings in order starting at index 0.
    private void GrowBuffer()
    {
        int newCapacity = _entries.Length * 2;
        ReadingEntry[] newEntries = new ReadingEntry[newCapacity];

        for (int i = 0; i < _count; i++)
        {
            int physIdx = (_head + i) % _entries.Length;
            newEntries[i] = _entries[physIdx];
        }

        _entries = newEntries;
        _head = 0;
    }
}
