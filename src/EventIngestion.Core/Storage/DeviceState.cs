namespace EventIngestion.Core.Storage;

using EventIngestion.Core.Models;

// Tracks the state and readings for a single IoT device in memory.
// Uses a private lock so that operations on this device never block other devices.
public sealed class DeviceState
{
    private readonly object _syncRoot = new object();
    private readonly DeviceCircularBuffer _buffer = new DeviceCircularBuffer();

    public string DeviceId { get; }
    public long LastActivityUtcMs { get; private set; }

    public DeviceState(string deviceId, long initialActivityUtcMs)
    {
        DeviceId = deviceId;
        LastActivityUtcMs = initialActivityUtcMs;
    }

    // Safely adds a reading to the buffer under lock to avoid concurrency issues.
    public bool Ingest(ReadingEntry reading, long cutoffUtcMs, long nowUtcMs)
    {
        lock (_syncRoot)
        {
            LastActivityUtcMs = Math.Max(LastActivityUtcMs, nowUtcMs);
            return _buffer.TryInsert(reading, cutoffUtcMs);
        }
    }

    // Prunes expired data and returns current aggregates (count, min, max, average).
    public bool TryGetAggregates(long cutoffUtcMs, out DeviceAggregates aggregates)
    {
        lock (_syncRoot)
        {
            _buffer.PruneOlderThan(cutoffUtcMs);
            return _buffer.TryGetAggregates(out aggregates);
        }
    }

    // Checks if this device has no readings and has been inactive long enough to delete.
    public bool PruneAndCheckIfIdle(long cutoffUtcMs, long idleCutoffUtcMs)
    {
        lock (_syncRoot)
        {
            _buffer.PruneOlderThan(cutoffUtcMs);
            return _buffer.Count == 0 && LastActivityUtcMs < idleCutoffUtcMs;
        }
    }

    // Helper used in tests to check how many active readings exist right now.
    public int GetActiveCount(long cutoffUtcMs)
    {
        lock (_syncRoot)
        {
            _buffer.PruneOlderThan(cutoffUtcMs);
            return _buffer.Count;
        }
    }
}
