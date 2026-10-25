using EventIngestion.Api.BackgroundServices;
using EventIngestion.Core.Common;
using EventIngestion.Core.Storage;

var builder = WebApplication.CreateBuilder(args);

builder.WebHost.ConfigureKestrel(options =>
{
    options.Limits.MaxRequestBodySize = 100 * 1024 * 1024;
    options.Limits.MinRequestBodyDataRate = null;
});

var windowOptions = new WindowOptions();
builder.Configuration.GetSection("WindowOptions").Bind(windowOptions);
builder.Services.AddSingleton(windowOptions);

builder.Services.AddSingleton<ITimeProvider>(SystemTimeProvider.Instance);
builder.Services.AddSingleton<DeviceIdCache>();
builder.Services.AddSingleton<IDeviceStore, InMemoryDeviceStore>();

builder.Services.AddHostedService<WindowEvictionBackgroundService>();

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.OperationFilter<EventIngestion.Api.Swagger.StreamingBatchOperationFilter>();
});

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI(c =>
{
    c.SwaggerEndpoint("/swagger/v1/swagger.json", "Event Ingestion API v1");
    c.RoutePrefix = string.Empty;
});

app.UseAuthorization();
app.MapControllers();

app.Run();

public partial class Program { }
