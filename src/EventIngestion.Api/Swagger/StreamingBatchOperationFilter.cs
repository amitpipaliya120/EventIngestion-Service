namespace EventIngestion.Api.Swagger;

using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using Swashbuckle.AspNetCore.SwaggerGen;
using EventIngestion.Api.Controllers;

// Configures the Swagger UI to show the request body schema and sample data for the streaming batch endpoint.
public sealed class StreamingBatchOperationFilter : IOperationFilter
{
    // Attaches the JSON schema and pre-filled sample payload to IngestBatch in Swagger.
    public void Apply(OpenApiOperation operation, OperationFilterContext context)
    {
        if (context.MethodInfo.Name == nameof(IngestionController.IngestBatch))
        {
            string nowIso = DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ssZ");
            string pastIso = DateTimeOffset.UtcNow.AddMinutes(-1).ToString("yyyy-MM-ddTHH:mm:ssZ");

            string exampleJson = @$"[
              {{
                ""deviceId"": ""sensor-0042"",
                ""timestamp"": ""{nowIso}"",
                ""value"": 21.5
              }},
              {{
                ""deviceId"": ""sensor-0042"",
                ""timestamp"": ""{pastIso}"",
                ""value"": 24.5
              }}
            ]";

            operation.RequestBody = new OpenApiRequestBody
            {
                Description = "Batch of sensor readings (JSON array of up to 50,000 readings)",
                Required = true,
                Content = new Dictionary<string, OpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Type = "array",
                            Items = new OpenApiSchema
                            {
                                Type = "object",
                                Properties = new Dictionary<string, OpenApiSchema>
                                {
                                    ["deviceId"] = new OpenApiSchema { Type = "string", Example = new OpenApiString("sensor-0042") },
                                    ["timestamp"] = new OpenApiSchema { Type = "string", Format = "date-time", Example = new OpenApiString(nowIso) },
                                    ["value"] = new OpenApiSchema { Type = "number", Format = "double", Example = new OpenApiDouble(21.5) }
                                },
                                Required = new HashSet<string> { "deviceId", "timestamp", "value" }
                            }
                        },
                        Example = new OpenApiString(exampleJson)
                    }
                }
            };
        }
    }
}
