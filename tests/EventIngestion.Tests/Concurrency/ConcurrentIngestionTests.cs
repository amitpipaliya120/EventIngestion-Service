namespace EventIngestion.Tests.Concurrency;

using EventIngestion.Core.Common;
using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;
using FluentAssertions;
using Xunit;

public class ConcurrentIngestionTests
{
    [Fact]
    public async Task HighConcurrency_OverlappingDevices_NoLostUpdates()
    {
        var store = new InMemoryDeviceStore(SystemTimeProvider.Instance, new WindowOptions
        {
            WindowDuration = TimeSpan.FromMinutes(10)
        });

        const string targetDevice = "sensor-concurrent-01";
        const int threadCount = 10;
        const int itemsPerThread = 2000;
        long baseTimestampMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var tasks = new Task[threadCount];

        for (int t = 0; t < threadCount; t++)
        {
            int threadId = t;
            tasks[t] = Task.Run(() =>
            {
                for (int i = 0; i < itemsPerThread; i++)
                {
                    long itemTimestamp = baseTimestampMs + (threadId * itemsPerThread) + i;
                    store.IngestReading(targetDevice, new ReadingEntry(itemTimestamp, 10.0));
                }
            });
        }

        await Task.WhenAll(tasks);

        bool found = store.TryGetAggregates(targetDevice, out var aggs);
        found.Should().BeTrue();
        aggs.Count.Should().Be(threadCount * itemsPerThread, "Every reading must be recorded with no lost updates");
        aggs.Min.Should().Be(10.0);
        aggs.Max.Should().Be(10.0);
        aggs.Average.Should().Be(10.0);
    }

    [Fact]
    public async Task ConcurrentIngestionAndQueries_MaintainsConsistentStateWithoutTornReads()
    {
        var store = new InMemoryDeviceStore(SystemTimeProvider.Instance, new WindowOptions
        {
            WindowDuration = TimeSpan.FromMinutes(5)
        });

        const string targetDevice = "sensor-torn-read-test";
        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
        long baseTimestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        int writeCounter = 0;

        // Writer task
        var writer = Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                int val = Interlocked.Increment(ref writeCounter);
                store.IngestReading(targetDevice, new ReadingEntry(baseTimestamp + val, val));
                Thread.Yield();
            }
        });

        // Multiple reader tasks checking invariants
        var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
        {
            while (!cts.Token.IsCancellationRequested)
            {
                if (store.TryGetAggregates(targetDevice, out var aggs))
                {
                    // Assert structural invariants
                    aggs.Count.Should().BeGreaterThan(0);
                    aggs.Min.Should().BeLessOrEqualTo(aggs.Max);
                    aggs.Average.Should().BeGreaterOrEqualTo(aggs.Min);
                    aggs.Average.Should().BeLessOrEqualTo(aggs.Max);
                }
                Thread.Yield();
            }
        })).ToArray();

        await Task.WhenAll([writer, .. readers]);
    }
}
