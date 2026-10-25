namespace EventIngestion.Benchmarks;

using System.IO.Pipelines;
using System.Text;
using System.Text.Json;
using BenchmarkDotNet.Attributes;
using EventIngestion.Api.Streaming;
using EventIngestion.Core.Common;
using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;

public class PocoReading
{
    public string DeviceId { get; set; } = string.Empty;
    public DateTimeOffset Timestamp { get; set; }
    public double Value { get; set; }
}

[MemoryDiagnoser]
[ShortRunJob]
public class IngestionBenchmarks
{
    private byte[] _payloadBytes = null!;
    private IDeviceStore _store = null!;
    private DeviceIdCache _cache = null!;

    [Params(10_000, 50_000)]
    public int BatchSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _store = new InMemoryDeviceStore(SystemTimeProvider.Instance);
        _cache = new DeviceIdCache();

        var sb = new StringBuilder(BatchSize * 80);
        DateTimeOffset now = DateTimeOffset.UtcNow;
        sb.Append('[');
        for (int i = 0; i < BatchSize; i++)
        {
            if (i > 0) sb.Append(',');
            string devId = $"sensor-{(i % 1000):D4}";
            sb.Append("{\"deviceId\":\"")
              .Append(devId)
              .Append("\",\"timestamp\":\"")
              .Append(now.AddSeconds(-(i % 200)).ToString("O"))
              .Append("\",\"value\":")
              .Append(20.0 + (i % 30))
              .Append('}');
        }
        sb.Append(']');

        _payloadBytes = Encoding.UTF8.GetBytes(sb.ToString());

        // Warm up cache with device IDs
        for (int i = 0; i < 1000; i++)
        {
            _cache.GetOrAdd(Encoding.UTF8.GetBytes($"sensor-{i:D4}"));
        }
    }

    [Benchmark(Baseline = true, Description = "Standard JsonSerializer Materialization (POCO List)")]
    public int Benchmark_StandardJsonSerializer()
    {
        var list = JsonSerializer.Deserialize<List<PocoReading>>(_payloadBytes)!;
        int count = 0;
        foreach (var item in list)
        {
            if (_store.IngestReading(item.DeviceId, new ReadingEntry(item.Timestamp.ToUnixTimeMilliseconds(), item.Value)))
            {
                count++;
            }
        }
        return count;
    }

    [Benchmark(Description = "Zero-Materialization Streaming Utf8JsonReader + DeviceIdCache")]
    public async Task<int> Benchmark_StreamingUtf8Reader()
    {
        var pipeReader = PipeReader.Create(new MemoryStream(_payloadBytes));
        var result = await JsonBatchStreamReader.ReadBatchAsync(pipeReader, _store, _cache);
        return result.Accepted;
    }
}
