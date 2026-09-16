namespace ActorClient.Services;

/// <summary>
/// Synthetic load generator: periodically raises the desired-instance count
/// to simulate new work arriving that needs its own actor instance. This is
/// the demand signal KEDA polls via /metrics/desired-instances to scale the
/// actor-host StatefulSet up one pod at a time. You can also drive demand
/// on-demand via POST /demand/add instead of waiting for this loop.
/// </summary>
public sealed class DemandGeneratorWorker(
    JobQueue jobQueue,
    IConfiguration configuration,
    ILogger<DemandGeneratorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var enabled = bool.TryParse(configuration["DEMAND_GENERATOR_ENABLED"], out var e) ? e : true;
        if (!enabled)
        {
            logger.LogInformation("Synthetic demand generator disabled (DEMAND_GENERATOR_ENABLED=false)");
            return;
        }

        var intervalSeconds = int.TryParse(configuration["DEMAND_GENERATOR_INTERVAL_SECONDS"], out var s) ? s : 20;

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), stoppingToken);

            var updated = jobQueue.IncreaseDemand();
            logger.LogInformation("Synthetic demand generator: desiredInstanceCount -> {Count}", updated);
        }
    }
}
