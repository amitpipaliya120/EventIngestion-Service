namespace EventIngestion.Api.BackgroundServices;

using EventIngestion.Core.Common;
using EventIngestion.Core.Storage;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

// Background worker that runs every 10 seconds to clean up expired data and remove idle devices.
public sealed class WindowEvictionBackgroundService : BackgroundService
{
    private readonly IDeviceStore _store;
    private readonly WindowOptions _options;
    private readonly ILogger<WindowEvictionBackgroundService> _logger;

    public WindowEvictionBackgroundService(
        IDeviceStore store,
        WindowOptions options,
        ILogger<WindowEvictionBackgroundService> logger)
    {
        _store = store ?? throw new ArgumentNullException(nameof(store));
        _options = options ?? throw new ArgumentNullException(nameof(options));
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    // Runs for the lifetime of the application, waking up on each timer tick to prune inactive devices.
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_options.EvictionInterval);

        while (!stoppingToken.IsCancellationRequested && await timer.WaitForNextTickAsync(stoppingToken))
        {
            try
            {
                int evicted = _store.PruneInactiveDevices();
                if (evicted > 0)
                {
                    _logger.LogInformation(
                        "Periodic eviction pruned {EvictedCount} inactive devices. Total devices remaining: {DeviceCount}",
                        evicted,
                        _store.DeviceCount
                    );
                }
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                _logger.LogError(ex, "Error occurred during periodic sliding window eviction.");
            }
        }
    }
}
