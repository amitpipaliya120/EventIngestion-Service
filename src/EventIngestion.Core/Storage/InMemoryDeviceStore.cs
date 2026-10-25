namespace EventIngestion.Core.Storage;

using System.Collections.Concurrent;
using EventIngestion.Core.Common;
using EventIngestion.Core.Models;

// Main in-memory store that holds all active devices in a thread-safe dictionary.
public sealed class InMemoryDeviceStore : IDeviceStore
{
    private readonly ConcurrentDictionary<string, DeviceState> _devices = new(StringComparer.Ordinal);
    private readonly ITimeProvider _timeProvider;
    private readonly long _windowDurationMs;
    private readonly long _deviceTtlMs;

    public InMemoryDeviceStore(ITimeProvider timeProvider, WindowOptions? options = null)
    {
        _timeProvider = timeProvider ?? throw new ArgumentNullException(nameof(timeProvider));
        options ??= new WindowOptions();
        _windowDurationMs = (long)options.WindowDuration.TotalMilliseconds;
        _deviceTtlMs = (long)options.DeviceTtlAfterExpiry.TotalMilliseconds;
    }

    // Total number of devices currently stored in memory.
    public int DeviceCount
    {
        get
        {
            return _devices.Count;
        }
    }

    // Returns true if this device has been registered in memory.
    public bool ContainsDevice(string deviceId)
    {
        return _devices.ContainsKey(deviceId);
    }

    // Finds the device (or creates it on first arrival) and adds the reading.
    public bool IngestReading(string deviceId, ReadingEntry reading)
    {
        long nowUtcMs = _timeProvider.GetUtcNowMs();
        long cutoffUtcMs = nowUtcMs - _windowDurationMs;

        DeviceState state = _devices.GetOrAdd(
            deviceId,
            CreateDeviceState,
            nowUtcMs
        );

        return state.Ingest(reading, cutoffUtcMs, nowUtcMs);
    }

    // Factory method passed to GetOrAdd to avoid delegate closure allocation.
    private static DeviceState CreateDeviceState(string id, long now)
    {
        return new DeviceState(id, now);
    }

    // Returns the current 5-minute rolling aggregates for the requested device.
    public bool TryGetAggregates(string deviceId, out DeviceAggregates aggregates)
    {
        long nowUtcMs = _timeProvider.GetUtcNowMs();
        long cutoffUtcMs = nowUtcMs - _windowDurationMs;

        if (!_devices.TryGetValue(deviceId, out DeviceState? state))
        {
            aggregates = default;
            return false;
        }

        return state.TryGetAggregates(cutoffUtcMs, out aggregates);
    }

    // Called every 10 seconds by background service to remove empty, idle devices from memory.
    public int PruneInactiveDevices()
    {
        long nowUtcMs = _timeProvider.GetUtcNowMs();
        long cutoffUtcMs = nowUtcMs - _windowDurationMs;
        long idleCutoffUtcMs = nowUtcMs - _deviceTtlMs;

        int removedCount = 0;
        foreach (KeyValuePair<string, DeviceState> kvp in _devices)
        {
            if (kvp.Value.PruneAndCheckIfIdle(cutoffUtcMs, idleCutoffUtcMs))
            {
                if (_devices.TryRemove(kvp))
                {
                    removedCount++;
                }
            }
        }

        return removedCount;
    }
}
