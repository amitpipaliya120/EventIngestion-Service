namespace EventIngestion.Api.Controllers;

using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using EventIngestion.Api.Streaming;
using EventIngestion.Core.Models;
using EventIngestion.Core.Storage;

// Handles high-volume batch ingestion of sensor readings.
[ApiController]
[Route("api/readings")]
public sealed class IngestionController : ControllerBase
{
    private readonly IDeviceStore _store;
    private readonly DeviceIdCache _cache;
    private readonly ILogger<IngestionController> _logger;

    public IngestionController(IDeviceStore store, DeviceIdCache cache, ILogger<IngestionController> logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _cache = cache ?? throw new ArgumentNullException(nameof(cache));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    // Accepts up to 50,000 readings via POST and streams them directly without materializing a List.
    [HttpPost("batch")]
    public async Task<IActionResult> IngestBatch(CancellationToken cancellationToken)
    {
        if (!Request.HasJsonContentType())
        {
            return StatusCode(StatusCodes.Status415UnsupportedMediaType, new
            {
                error = "Content-Type must be application/json."
            });
        }

        try
        {
            IngestionBatchResult result = await JsonBatchStreamReader.ReadBatchAsync(
                Request.BodyReader,
                _store,
                _cache,
                cancellationToken
            );

            return Ok(result);
        }
        catch (JsonException ex)
        {
            _logger.LogWarning(ex, "Failed to parse JSON streaming payload.");
            return BadRequest(new
            {
                error = "Malformed JSON payload.",
                details = ex.Message
            });
        }
    }
}
