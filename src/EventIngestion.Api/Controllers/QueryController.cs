namespace EventIngestion.Api.Controllers;

using Microsoft.AspNetCore.Mvc;
using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;

// Exposes query endpoints to fetch rolling 5-minute aggregates for devices.
[ApiController]
[Route("api/devices")]
public sealed class QueryController : ControllerBase
{
    private readonly IDeviceStore _store;

    public QueryController(IDeviceStore store)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
    }

    // Returns count, min, max, and average for the specified device over the last 5 minutes.
    [HttpGet("{deviceId}/aggregates")]
    public IActionResult GetAggregates(string deviceId)
    {
        if (string.IsNullOrWhiteSpace(deviceId))
        {
            return BadRequest(new { error = "deviceId must not be empty." });
        }

        if (!_store.TryGetAggregates(deviceId, out DeviceAggregates aggregates))
        {
            string message = _store.ContainsDevice(deviceId)
                ? $"Device '{deviceId}' has no active readings within the last 5 minutes."
                : $"Device '{deviceId}' is unknown.";

            return NotFound(new
            {
                deviceId,
                message
            });
        }

        return Ok(aggregates);
    }
}
