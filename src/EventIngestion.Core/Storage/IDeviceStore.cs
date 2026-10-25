namespace EventIngestion.Core.Storage;

using EventIngestion.Core.Models;

// Main contract for device storage operations.
// Defines methods for ingesting events, querying aggregates, and background cleanup.
public interface IDeviceStore
{
    // Inserts a sensor reading into the target device's sliding window.
    // Returns true if accepted, or false if already older than 5 minutes.
    bool IngestReading(string deviceId, ReadingEntry reading);

    // Calculates and returns rolling metrics (count, min, max, average) for a device.
    // Returns false if the device is unknown or has no active readings.
    bool TryGetAggregates(string deviceId, out DeviceAggregates aggregates);

    // Iterates through all devices, removes expired readings, and deletes idle devices.
    // Returns the total number of removed devices to reclaim RAM.
    int PruneInactiveDevices();

    // Quickly checks if a device ID currently exists in the store.
    // Used to differentiate between unknown devices and expired devices.
    bool ContainsDevice(string deviceId);

    // Returns the total count of devices currently held in RAM.
    int DeviceCount { get; }
}
