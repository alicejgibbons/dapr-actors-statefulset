using ActorContracts;
using Dapr.Actors;
using Dapr.Actors.Client;

namespace ActorClient.Services;

/// <summary>
/// The single actor client's dispatch loop. Every couple of seconds it asks
/// <see cref="JobQueue"/> to assign any pending jobs to idle, currently-known
/// actor types/pods, then fires off DoWorkAsync for each newly-assigned job
/// concurrently. When a call returns, the job's slot is freed immediately --
/// that's what drops JobQueue.DesiredInstanceCount and lets KEDA scale the
/// corresponding pod back down once its job is done.
/// </summary>
public sealed class ActorInvokerWorker(
    JobQueue jobQueue,
    IConfiguration configuration,
    ILogger<ActorInvokerWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var assignIntervalSeconds = int.TryParse(configuration["ASSIGN_INTERVAL_SECONDS"], out var s) ? s : 2;

        while (!stoppingToken.IsCancellationRequested)
        {
            foreach (var assignment in jobQueue.AssignPendingJobs())
            {
                _ = RunJobAsync(assignment, stoppingToken);
            }

            await Task.Delay(TimeSpan.FromSeconds(assignIntervalSeconds), stoppingToken);
        }
    }

    private async Task RunJobAsync(ActorInvocationJob assignment, CancellationToken stoppingToken)
    {
        var succeeded = false;
        try
        {
            var actorId = new ActorId($"job-{assignment.Ordinal}");
            var proxy = ActorProxy.Create<IWorkerActor>(actorId, assignment.ActorTypeName);

            logger.LogInformation(
                "Dispatching job {JobId} ({DurationMs}ms) to {ActorType}/{ActorId}",
                assignment.Job.JobId, assignment.Job.SimulatedDurationMs, assignment.ActorTypeName, actorId);

            var result = await proxy.DoWorkAsync(new WorkItem(assignment.Job.JobId, assignment.Job.SimulatedDurationMs));

            logger.LogInformation(
                "Job {JobId} finished on {ActorType}/{ActorId} (pod {PodName}) -> invocationCount={Count}",
                result.JobId, result.ActorType, result.ActorId, result.PodName, result.InvocationCount);

            succeeded = true;
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Job {JobId} failed on actor type {ActorType} (ordinal {Ordinal})",
                assignment.Job.JobId, assignment.ActorTypeName, assignment.Ordinal);
        }
        finally
        {
            jobQueue.CompleteJob(assignment.Ordinal, succeeded);
        }
    }
}
