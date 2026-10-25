namespace EventIngestion.Tests.Unit;

using EventIngestion.Core.Common;
using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;
using EventIngestion.Tests.Helpers;
using FluentAssertions;
using Xunit;

public class DeviceStateTests
{
    [Fact]
    public void SlidingWindow_ContinuouslyAgesOut_WhenTimeAdvances()
    {
        var startTime = new DateTimeOffset(2026, 10, 24, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new TestTimeProvider(startTime);
        var options = new WindowOptions { WindowDuration = TimeSpan.FromMinutes(5) };
        var store = new InMemoryDeviceStore(timeProvider, options);

        string deviceId = "device-test-01";

        // T0: insert 3 readings at 12:00:00, 12:01:00, 12:02:00
        store.IngestReading(deviceId, new ReadingEntry(startTime.ToUnixTimeMilliseconds(), 10.0));
        store.IngestReading(deviceId, new ReadingEntry(startTime.AddMinutes(1).ToUnixTimeMilliseconds(), 20.0));
        store.IngestReading(deviceId, new ReadingEntry(startTime.AddMinutes(2).ToUnixTimeMilliseconds(), 30.0));

        // At 12:02:00 (T+2m): all 3 readings must be in the window
        timeProvider.SetUtcNow(startTime.AddMinutes(2));
        bool found = store.TryGetAggregates(deviceId, out var aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(3);
        aggs.Min.Should().Be(10.0);
        aggs.Max.Should().Be(30.0);
        aggs.Average.Should().Be(20.0);

        // At 12:05:30 (T+5.5m): reading 1 (at 12:00:00) has aged out! (window cutoff is 12:00:30)
        timeProvider.SetUtcNow(startTime.AddMinutes(5).AddSeconds(30));
        found = store.TryGetAggregates(deviceId, out aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(2); // Only readings from 12:01:00 and 12:02:00
        aggs.Min.Should().Be(20.0);
        aggs.Max.Should().Be(30.0);
        aggs.Average.Should().Be(25.0);

        // At 12:07:30 (T+7.5m): all readings have aged out! (window cutoff is 12:02:30)
        timeProvider.SetUtcNow(startTime.AddMinutes(7).AddSeconds(30));
        found = store.TryGetAggregates(deviceId, out aggs);
        found.Should().BeFalse(); // Device has no readings in active window
    }

    [Fact]
    public void PruneInactiveDevices_RemovesEmptyIdleDevices()
    {
        var startTime = new DateTimeOffset(2026, 10, 24, 12, 0, 0, TimeSpan.Zero);
        var timeProvider = new TestTimeProvider(startTime);
        var options = new WindowOptions
        {
            WindowDuration = TimeSpan.FromMinutes(5),
            DeviceTtlAfterExpiry = TimeSpan.FromMinutes(5)
        };
        var store = new InMemoryDeviceStore(timeProvider, options);

        store.IngestReading("dev-A", new ReadingEntry(startTime.ToUnixTimeMilliseconds(), 100.0));
        store.DeviceCount.Should().Be(1);

        // Advance 11 minutes (past 5 min window + 5 min TTL)
        timeProvider.SetUtcNow(startTime.AddMinutes(11));

        int pruned = store.PruneInactiveDevices();
        pruned.Should().Be(1);
        store.DeviceCount.Should().Be(0);
    }
}
