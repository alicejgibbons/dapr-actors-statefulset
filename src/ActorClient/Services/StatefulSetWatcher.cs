using System.Text.RegularExpressions;
using k8s;

namespace ActorClient.Services;

/// <summary>
/// Polls the Kubernetes API for the actor-host StatefulSet's pods and keeps
/// <see cref="JobQueue"/>'s known-pod/actor-type map in sync. This is what lets
/// the single actor client discover new actor types automatically as KEDA
/// scales the StatefulSet up (or removes them as it scales down).
/// </summary>
public sealed class StatefulSetWatcher(
    IKubernetes kubernetes,
    JobQueue jobQueue,
    IConfiguration configuration,
    ILogger<StatefulSetWatcher> logger) : BackgroundService
{
    private static readonly Regex OrdinalSuffix = new(@"-(\d+)$", RegexOptions.Compiled);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var podNamespace = configuration["POD_NAMESPACE"] ?? "actors-demo";
        var labelSelector = configuration["ACTOR_HOST_LABEL_SELECTOR"] ?? "app=actor-host";
        var actorTypePrefix = configuration["ACTOR_TYPE_PREFIX"] ?? "WorkerActor";
        var pollSeconds = int.TryParse(configuration["WATCH_POLL_SECONDS"], out var s) ? s : 5;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var pods = await kubernetes.CoreV1.ListNamespacedPodAsync(
                    podNamespace,
                    labelSelector: labelSelector,
                    cancellationToken: stoppingToken);

                var observed = new Dictionary<int, string>();

                foreach (var pod in pods.Items)
                {
                    var isReady = pod.Status?.ContainerStatuses?.All(c => c.Ready) == true
                        && pod.Status?.Phase == "Running";

                    if (!isReady)
                    {
                        continue;
                    }

                    var match = OrdinalSuffix.Match(pod.Metadata.Name);
                    if (!match.Success)
                    {
                        continue;
                    }

                    var ordinal = int.Parse(match.Groups[1].Value);
                    observed[ordinal] = $"{actorTypePrefix}-{ordinal}";
                }

                var (added, removed) = jobQueue.SyncKnownPods(observed);

                foreach (var ordinal in added)
                {
                    logger.LogInformation(
                        "Discovered actor-host-{Ordinal} hosting actor type {ActorType}",
                        ordinal, observed[ordinal]);
                }

                foreach (var ordinal in removed)
                {
                    logger.LogInformation("actor-host-{Ordinal} is gone; dropping its actor type", ordinal);
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(ex, "Failed to list actor-host pods");
            }

            await Task.Delay(TimeSpan.FromSeconds(pollSeconds), stoppingToken);
        }
    }
}
