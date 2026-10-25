namespace EventIngestion.Tests.Integration;

using System.Net;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json;
using EventIngestion.Core.Models;
using FluentAssertions;
using Microsoft.AspNetCore.Mvc.Testing;
using Xunit;

public class ApiIntegrationTests : IClassFixture<WebApplicationFactory<Program>>
{
    private readonly HttpClient _client;

    public ApiIntegrationTests(WebApplicationFactory<Program> factory)
    {
        _client = factory.CreateClient();
    }

    [Fact]
    public async Task PostBatch_AcceptsPayloadAndComputesAggregates()
    {
        string deviceId = "test-device-batch-01";
        DateTimeOffset now = DateTimeOffset.UtcNow;

        var readings = new[]
        {
            new { deviceId, timestamp = now.AddMinutes(-2).ToString("O"), value = 10.0 },
            new { deviceId, timestamp = now.AddMinutes(-1).ToString("O"), value = 30.0 },
            new { deviceId, timestamp = now.ToString("O"), value = 20.0 }
        };

        string json = JsonSerializer.Serialize(readings);
        using var content = new StringContent(json, Encoding.UTF8, "application/json");

        // Act: POST batch
        var postResponse = await _client.PostAsync("/api/readings/batch", content);
        postResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var batchResult = await postResponse.Content.ReadFromJsonAsync<IngestionBatchResult>();
        batchResult.Should().NotBeNull();
        batchResult!.Accepted.Should().Be(3);

        // Act: GET aggregates
        var getResponse = await _client.GetAsync($"/api/devices/{deviceId}/aggregates");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var aggregates = await getResponse.Content.ReadFromJsonAsync<DeviceAggregates>();
        aggregates.Should().NotBeNull();
        aggregates!.Count.Should().Be(3);
        aggregates.Min.Should().Be(10.0);
        aggregates.Max.Should().Be(30.0);
        aggregates.Average.Should().Be(20.0);
    }

    [Fact]
    public async Task PostBatch_With50000Readings_SucceedsStreaming()
    {
        string deviceId = "sensor-massive-load";
        DateTimeOffset now = DateTimeOffset.UtcNow;
        const int count = 50_000;

        // Build a JSON stream carrying 50,000 readings
        var sb = new StringBuilder(count * 80);
        sb.Append('[');
        for (int i = 0; i < count; i++)
        {
            if (i > 0) sb.Append(',');
            sb.Append("{\"deviceId\":\"")
              .Append(deviceId)
              .Append("\",\"timestamp\":\"")
              .Append(now.AddSeconds(- (i % 240)).ToString("O"))
              .Append("\",\"value\":")
              .Append(10.0 + (i % 50))
              .Append('}');
        }
        sb.Append(']');

        using var content = new StringContent(sb.ToString(), Encoding.UTF8, "application/json");

        var response = await _client.PostAsync("/api/readings/batch", content);
        response.StatusCode.Should().Be(HttpStatusCode.OK);

        var batchResult = await response.Content.ReadFromJsonAsync<IngestionBatchResult>();
        batchResult.Should().NotBeNull();
        batchResult!.Accepted.Should().Be(count);

        // Query aggregates
        var getResponse = await _client.GetAsync($"/api/devices/{deviceId}/aggregates");
        getResponse.StatusCode.Should().Be(HttpStatusCode.OK);

        var aggs = await getResponse.Content.ReadFromJsonAsync<DeviceAggregates>();
        aggs.Should().NotBeNull();
        aggs!.Count.Should().Be(count);
    }

    [Fact]
    public async Task GetAggregates_UnknownDevice_ReturnsNotFound()
    {
        var response = await _client.GetAsync("/api/devices/nonexistent-device-xyz/aggregates");
        response.StatusCode.Should().Be(HttpStatusCode.NotFound);

        var body = await response.Content.ReadAsStringAsync();
        body.Should().Contain("Device 'nonexistent-device-xyz' is unknown.");
    }

    [Fact]
    public async Task PostBatch_MalformedJson_ReturnsBadRequest()
    {
        using var content = new StringContent("[ { invalid_json } ]", Encoding.UTF8, "application/json");
        var response = await _client.PostAsync("/api/readings/batch", content);
        response.StatusCode.Should().Be(HttpStatusCode.BadRequest);
    }
}
