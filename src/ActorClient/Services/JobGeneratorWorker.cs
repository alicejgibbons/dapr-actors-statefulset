namespace ActorClient.Services;

/// <summary>
/// Synthetic load generator: periodically submits a new job with a
/// realistic simulated duration, representing new work arriving that needs
/// its own actor instance. Because JobQueue.DesiredInstanceCount only counts
/// jobs that are still pending or running, once a job's simulated work
/// finishes its slot frees up on its own and KEDA scales the corresponding
/// pod back down -- no manual reset needed. Use POST /jobs to submit one on
/// demand instead of waiting for this loop.
/// </summary>
public sealed class JobGeneratorWorker(
    JobQueue jobQueue,
    IConfiguration configuration,
    ILogger<JobGeneratorWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var enabled = bool.TryParse(configuration["JOB_GENERATOR_ENABLED"], out var e) ? e : true;
        if (!enabled)
        {
            logger.LogInformation("Synthetic job generator disabled (JOB_GENERATOR_ENABLED=false)");
            return;
        }

        var intervalSeconds = int.TryParse(configuration["JOB_GENERATOR_INTERVAL_SECONDS"], out var s) ? s : 15;
        var minDurationMs = int.TryParse(configuration["JOB_DURATION_MS_MIN"], out var min) ? min : 20_000;
        var maxDurationMs = int.TryParse(configuration["JOB_DURATION_MS_MAX"], out var max) ? max : 35_000;

        while (!stoppingToken.IsCancellationRequested)
        {
            await Task.Delay(TimeSpan.FromSeconds(intervalSeconds), stoppingToken);

            if (jobQueue.DesiredInstanceCount >= JobQueue.MaxOutstandingJobs)
            {
                continue;
            }

            var durationMs = Random.Shared.Next(minDurationMs, maxDurationMs + 1);
            var job = jobQueue.Submit(durationMs);

            logger.LogInformation(
                "Synthetic job generator: submitted job {JobId} ({DurationMs}ms), desiredInstanceCount now {Count}",
                job.JobId, durationMs, jobQueue.DesiredInstanceCount);
        }
    }
}
