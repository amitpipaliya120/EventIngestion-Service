namespace EventIngestion.Tests.Unit;

using System.Text;
using EventIngestion.Core.Storage;
using FluentAssertions;
using Xunit;

public class DeviceIdCacheTests
{
    [Fact]
    public void GetOrAdd_ReturnsSameStringReference_ForIdenticalUtf8Spans()
    {
        var cache = new DeviceIdCache();
        byte[] bytes1 = "sensor-9999"u8.ToArray();
        byte[] bytes2 = "sensor-9999"u8.ToArray();

        string s1 = cache.GetOrAdd(bytes1);
        string s2 = cache.GetOrAdd(bytes2);

        s1.Should().Be("sensor-9999");
        s2.Should().Be("sensor-9999");
        object.ReferenceEquals(s1, s2).Should().BeTrue("Interning must return the exact same heap reference");
    }

    [Fact]
    public void GetOrAdd_ThreadSafeAcrossMultipleThreads()
    {
        var cache = new DeviceIdCache();
        string[] deviceIds = Enumerable.Range(0, 1000).Select(i => $"sensor-{i:D4}").ToArray();

        Parallel.For(0, 10000, i =>
        {
            string target = deviceIds[i % deviceIds.Length];
            byte[] bytes = Encoding.UTF8.GetBytes(target);
            string retrieved = cache.GetOrAdd(bytes);
            retrieved.Should().Be(target);
        });
    }
}
