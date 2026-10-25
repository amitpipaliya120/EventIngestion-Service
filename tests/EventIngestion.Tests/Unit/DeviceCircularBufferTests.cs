namespace EventIngestion.Tests.Unit;

using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;
using FluentAssertions;
using Xunit;

public class DeviceCircularBufferTests
{
    [Fact]
    public void TryInsert_InOrderReadings_MaintainsCorrectAggregates()
    {
        var buffer = new DeviceCircularBuffer(initialCapacity: 16);
        long baseTime = 1_000_000;
        long cutoff = baseTime - 300_000;

        buffer.TryInsert(new ReadingEntry(baseTime + 1000, 10.0), cutoff).Should().BeTrue();
        buffer.TryInsert(new ReadingEntry(baseTime + 2000, 20.0), cutoff).Should().BeTrue();
        buffer.TryInsert(new ReadingEntry(baseTime + 3000, 30.0), cutoff).Should().BeTrue();

        buffer.Count.Should().Be(3);

        bool found = buffer.TryGetAggregates(out var aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(3);
        aggs.Min.Should().Be(10.0);
        aggs.Max.Should().Be(30.0);
        aggs.Average.Should().Be(20.0);
    }

    [Fact]
    public void TryInsert_OutOfOrderReadings_InsertsInSortedOrderAndMaintainsAggregates()
    {
        var buffer = new DeviceCircularBuffer(initialCapacity: 16);
        long baseTime = 1_000_000;
        long cutoff = baseTime - 300_000;

        // Arrive out of order: t=3000, then t=1000, then t=2000
        buffer.TryInsert(new ReadingEntry(baseTime + 3000, 100.0), cutoff).Should().BeTrue();
        buffer.TryInsert(new ReadingEntry(baseTime + 1000, 10.0), cutoff).Should().BeTrue();
        buffer.TryInsert(new ReadingEntry(baseTime + 2000, 50.0), cutoff).Should().BeTrue();

        buffer.Count.Should().Be(3);

        bool found = buffer.TryGetAggregates(out var aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(3);
        aggs.Min.Should().Be(10.0);
        aggs.Max.Should().Be(100.0);
        aggs.Average.Should().BeApproximately(53.3333, 0.001);
    }

    [Fact]
    public void TryInsert_ReadingOlderThanCutoff_IsRejected()
    {
        var buffer = new DeviceCircularBuffer(initialCapacity: 16);
        long now = 1_000_000;
        long cutoff = now - 300_000; // 5 min ago

        // Event from 6 minutes ago
        bool accepted = buffer.TryInsert(new ReadingEntry(now - 360_000, 42.0), cutoff);
        accepted.Should().BeFalse();
        buffer.Count.Should().Be(0);
    }

    [Fact]
    public void PruneOlderThan_AgesOutExpiredReadings_RecalculatesMinMaxIfNeeded()
    {
        var buffer = new DeviceCircularBuffer(initialCapacity: 16);
        long baseTime = 1_000_000;

        // Min reading at baseTime + 1000
        buffer.TryInsert(new ReadingEntry(baseTime + 1000, 5.0), baseTime).Should().BeTrue();
        buffer.TryInsert(new ReadingEntry(baseTime + 5000, 15.0), baseTime).Should().BeTrue();
        buffer.TryInsert(new ReadingEntry(baseTime + 9000, 25.0), baseTime).Should().BeTrue();

        // Advance cutoff past the first reading (which was the min: 5.0)
        long newCutoff = baseTime + 3000;
        int pruned = buffer.PruneOlderThan(newCutoff);
        pruned.Should().Be(1);
        buffer.Count.Should().Be(2);

        // Aggregates should reflect new min (15.0), max (25.0), avg (20.0)
        bool found = buffer.TryGetAggregates(out var aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(2);
        aggs.Min.Should().Be(15.0);
        aggs.Max.Should().Be(25.0);
        aggs.Average.Should().Be(20.0);
    }

    [Fact]
    public void GrowBuffer_BeyondInitialCapacity_PreservesAllElements()
    {
        var buffer = new DeviceCircularBuffer(initialCapacity: 8);
        long baseTime = 1_000_000;

        for (int i = 0; i < 500; i++)
        {
            buffer.TryInsert(new ReadingEntry(baseTime + (i * 10), i), baseTime).Should().BeTrue();
        }

        buffer.Count.Should().Be(500);

        bool found = buffer.TryGetAggregates(out var aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(500);
        aggs.Min.Should().Be(0.0);
        aggs.Max.Should().Be(499.0);
    }
}
