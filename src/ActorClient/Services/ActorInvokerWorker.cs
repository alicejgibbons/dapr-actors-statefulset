using ActorContracts;
using Dapr.Actors;
using Dapr.Actors.Client;

namespace ActorClient.Services;

/// <summary>
/// The single actor client. Every few seconds it enqueues one invocation job
/// per currently-known actor type/pod, then drains the queue and calls
/// DoWorkAsync on each through a Dapr actor proxy -- proving that one client
/// can reach every distinct actor type hosted across the StatefulSet.
/// </summary>
public sealed class ActorInvokerWorker(
    JobQueue jobQueue,
    IConfiguration configuration,
    ILogger<ActorInvokerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var invokeIntervalSeconds = int.TryParse(configuration["INVOKE_INTERVAL_SECONDS"], out var s) ? s : 5;

        var producer = ProduceJobsAsync(invokeIntervalSeconds, stoppingToken);
        var consumer = ConsumeJobsAsync(stoppingToken);

        await Task.WhenAll(producer, consumer);
    }

    private async Task ProduceJobsAsync(int invokeIntervalSeconds, CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var (ordinal, actorType) in jobQueue.KnownActorTypes)
            {
                jobQueue.TryEnqueue(new ActorInvocationJob(ordinal, actorType));
            }

            await Task.Delay(TimeSpan.FromSeconds(invokeIntervalSeconds), stoppingToken);
        }
    }

    private async Task ConsumeJobsAsync(CancellationToken stoppingToken)
    {
        await foreach (var job in jobQueue.DequeueAllAsync(stoppingToken))
        {
            try
            {
                var actorId = new ActorId($"job-{job.Ordinal}");
                var proxy = ActorProxy.Create<IWorkerActor>(actorId, job.ActorTypeName);

                var jobId = Guid.NewGuid().ToString("N")[..8];
                var result = await proxy.DoWorkAsync(new WorkItem(jobId));

                logger.LogInformation(
                    "Invoked {ActorType}/{ActorId} on pod {PodName} -> invocationCount={Count}",
                    result.ActorType, result.ActorId, result.PodName, result.InvocationCount);
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to invoke actor type {ActorType} (ordinal {Ordinal})",
                    job.ActorTypeName, job.Ordinal);
            }
        }
    }
}
