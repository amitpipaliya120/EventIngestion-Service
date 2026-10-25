namespace EventIngestion.Core.Common;

// Configuration settings for the 5-minute sliding window.
public sealed class WindowOptions
{
    // How long readings stay active (5 minutes).
    public TimeSpan WindowDuration { get; set; } = TimeSpan.FromMinutes(5);

    // How often the background cleaner runs (every 10 seconds).
    public TimeSpan EvictionInterval { get; set; } = TimeSpan.FromSeconds(10);

    // How long to wait before deleting an inactive device from memory (5 minutes).
    public TimeSpan DeviceTtlAfterExpiry { get; set; } = TimeSpan.FromMinutes(5);
}
