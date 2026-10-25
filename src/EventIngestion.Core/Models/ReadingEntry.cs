namespace EventIngestion.Core.Models;

using System.Runtime.InteropServices;

// Stores a single sensor reading (timestamp and value) using only 16 bytes of memory.
[StructLayout(LayoutKind.Sequential)]
public readonly record struct ReadingEntry(long TimestampUtcMs, double Value)
{
    // Converts the millisecond timestamp into a readable date and time.
    public DateTimeOffset Timestamp
    {
        get
        {
            return DateTimeOffset.FromUnixTimeMilliseconds(TimestampUtcMs);
        }
    }
}
