namespace EventIngestion.Core.Models;

// Holds the 4 required aggregate numbers for a device over the last 5 minutes.
public readonly record struct DeviceAggregates(
    long Count,
    double Min,
    double Max,
    double Average
);
